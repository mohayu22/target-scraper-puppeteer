const puppeteer = require('puppeteer');
const { parse } = require('json2csv');
const winston = require('winston');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const LOCATION = "us";
const keywords = ["apple pen"];
const MAX_PAGES = 1;
const MAX_THREADS = 2;
const MAX_RETRIES = 2;

const { api_key: API_KEY } = JSON.parse(fs.readFileSync('config.json', 'utf8'));

function getScrapeOpsUrl(url, location = LOCATION) {
    const params = new URLSearchParams({
        api_key: API_KEY,
        url,
        country: location,
        wait: 5000,
    });
    return `https://proxy.scrapeops.io/v1/?${params.toString()}`;
}

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.printf(({ timestamp, level, message }) => {
            return `${timestamp} [${level.toUpperCase()}]: ${message}`;
        })
    ),
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'target-scraper.log' })
    ]
});

class ProductSearchData {
    constructor(data = {}) {
        this.title = this.validateString(data.title, "No Title");
        this.url = this.validateString(data.url, "No URL");
    }

    validateString(value, fallback) {
        return typeof value === 'string' && value.trim() !== '' ? value.trim() : fallback;
    }
}

class ProductData {
    constructor(data = {}) {
        this.title = this.validateString(data.title, "No Title");
        this.price = this.parsePrice(data.price);
        this.rating = this.parseRating(data.rating);
        this.reviewCount = this.parseReviewCount(data.reviewCount);
        this.details = this.validateString(data.details, "No Details");
    }

    validateString(value, fallback) {
        return typeof value === 'string' && value.trim() !== '' ? value.trim() : fallback;
    }

    parsePrice(value) {
        const priceMatch = typeof value === 'string' ? value.match(/\d+(\.\d{1,2})?/) : null;
        return priceMatch ? parseFloat(priceMatch[0]) : 0;
    }

    parseRating(value) {
        return typeof value === 'string' && !isNaN(parseFloat(value)) ? parseFloat(value) : 0;
    }

    parseReviewCount(value) {
        return typeof value === 'string' && !isNaN(parseInt(value)) ? parseInt(value) : 0;
    }
}

class DataPipeline {
    constructor(csvFilename, storageQueueLimit = 50) {
        this.namesSeen = new Set();
        this.storageQueue = [];
        this.storageQueueLimit = storageQueueLimit;
        this.csvFilename = csvFilename;
    }

    async saveToCsv() {
        const filePath = path.resolve(this.csvFilename);
        const dataToSave = this.storageQueue.splice(0, this.storageQueue.length);
        if (dataToSave.length === 0) return;

        const csvData = parse(dataToSave, { header: !fs.existsSync(filePath) });
        fs.appendFileSync(filePath, csvData + '\n', 'utf8');
    }

    isDuplicate(title) {
        if (this.namesSeen.has(title)) {
            logger.warn(`Duplicate item found: ${title}. Item dropped.`);
            return true;
        }
        this.namesSeen.add(title);
        return false;
    }

    async addData(data) {
        if (!this.isDuplicate(data.title)) {
            this.storageQueue.push(data);
            if (this.storageQueue.length >= this.storageQueueLimit) {
                await this.saveToCsv();
            }
        }
    }

    async closePipeline() {
        if (this.storageQueue.length > 0) await this.saveToCsv();
    }
}

async function scrapeSearchData(datapipeline, url, retries = MAX_RETRIES) {
    let tries = 0;
    let success = false;

    while (tries <= retries && !success) {
        try {
            const browser = await puppeteer.launch({ headless: false });
            const page = await browser.newPage();
            await page.setExtraHTTPHeaders({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.5; rv:109.0) Gecko/20100101 Firefox/117.0",
            });

            console.log(`Navigating to: ${url}`);
            await page.goto(getScrapeOpsUrl(url), { waitUntil: 'networkidle2' });

            await autoScroll(page);

            const productData = await page.evaluate(() => {
                const results = [];
                document.querySelectorAll("div[data-test='@web/site-top-of-funnel/ProductCardWrapper']").forEach(card => {
                    const linkElement = card.querySelector("a[href]");
                    if (linkElement) {
                        const href = linkElement.getAttribute("href");
                        const title = href.split("/")[2];
                        const url = `https://www.target.com${href}`;
                        results.push({ title, url });
                    }
                });
                return results;
            });

            for (const product of productData) {
                await datapipeline.addData(new ProductSearchData(product));
            }

            console.log(`Successfully scraped data from: ${url}`);

            await browser.close();
            success = true;
        } catch (error) {
            console.error(`Error scraping ${url}: ${error.message}`);
            console.log(`Retrying request for: ${url}, attempts left: ${retries - tries}`);
            tries += 1;
        }
    }

    if (!success) {
        throw new Error(`Max retries exceeded for ${url}`);
    }
}

async function scrapeProductData(datapipeline, url, retries = MAX_RETRIES) {
    let tries = 0;
    let success = false;

    while (tries <= retries && !success) {
        try {
            const browser = await puppeteer.launch({ headless: false });
            const page = await browser.newPage();
            await page.setExtraHTTPHeaders({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.5; rv:109.0) Gecko/20100101 Firefox/117.0",
            });

            console.log(`Navigating to: ${url}`);
            await page.goto(getScrapeOpsUrl(url), { waitUntil: 'domcontentloaded' });

            await autoScroll(page);

            const productData = await page.evaluate(() => {
                const title = document.querySelector("h1[data-test='product-title']")?.innerText || "N/A";
                const ratingHolder = document.querySelector("span[data-test='ratings']");
                const rating = ratingHolder ? ratingHolder.innerText.split(" ")[0] : "N/A";
                const reviewCount = ratingHolder ? ratingHolder.innerText.split(" ").slice(-2, -1)[0] : "0";
                const price = document.querySelector("span[data-test='product-price']")?.innerText || "N/A";
                const details = document.querySelector("div[data-test='productDetailTabs-itemDetailsTab']")?.innerText || "N/A";

                return { title, price, rating, reviewCount, details };
            });

            await datapipeline.addData(new ProductData(productData))
            console.log("Successfully scraped and saved product data");

            await browser.close();
            success = true;
        } catch (error) {
            console.error(`Error scraping ${url}: ${error.message}`);
            console.log(`Retrying request, attempts left: ${retries - tries}`);
            tries += 1;
        }
    }

    if (!success) {
        throw new Error(`Max retries exceeded for ${url}`);
    }
}

async function autoScroll(page) {
    await page.evaluate(async () => {
        await new Promise(resolve => {
            const distance = 100;
            const delay = 100;
            const scrollInterval = setInterval(() => {
                window.scrollBy(0, distance);
                if (window.innerHeight + window.scrollY >= document.body.scrollHeight) {
                    clearInterval(scrollInterval);
                    resolve();
                }
            }, delay);
        });
    });
}

async function readCsvAndGetUrls(csvFilename) {
    return new Promise((resolve, reject) => {
        const urls = [];
        fs.createReadStream(csvFilename)
            .pipe(csv())
            .on('data', (row) => {
                if (row.url) urls.push(row.url);
            })
            .on('end', () => resolve(urls))
            .on('error', reject);
    });
}

async function getAllUrlsFromFiles(files) {
    const urls = [];
    for (const file of files) {
        urls.push({ filename: file, urls: await readCsvAndGetUrls(file) });
    }
    return urls;
}

const scrapeConcurrently = async (tasks, maxConcurrency) => {
    const results = [];
    const executing = new Set();

    for (const task of tasks) {
        const promise = task().then(result => {
            executing.delete(promise);
            return result;
        });
        executing.add(promise);
        results.push(promise);

        if (executing.size >= maxConcurrency) {
            await Promise.race(executing);
        }
    }

    return Promise.all(results);
};

(async () => {
    logger.info("Started Scraping Search Data");
    const aggregateFiles = [];

    const scrapeSearchDataTasks = keywords.flatMap(keyword => {
        const tasks = [];
        for (let pageNumber = 0; pageNumber < MAX_PAGES; pageNumber++) {
            tasks.push(async () => {
                const formattedKeyword = encodeURIComponent(keyword);
                const csvFilename = `${keyword.replace(/\s+/g, '-').toLowerCase()}-page-${pageNumber}-search-data.csv`;
                const dataPipeline = new DataPipeline(csvFilename);
                const url = `https://www.target.com/s?searchTerm=${formattedKeyword}&Nao=${pageNumber * 24}`;

                await scrapeSearchData(dataPipeline, url);
                await dataPipeline.closePipeline();
                aggregateFiles.push(csvFilename);
            });
        }
        return tasks;
    });

    await scrapeConcurrently(scrapeSearchDataTasks, MAX_THREADS);

    const urls = await getAllUrlsFromFiles(aggregateFiles);

    logger.info("Started Scraping Product Data")

    const scrapeProductDataTasks = urls.flatMap(data =>
        data.urls.map(url => async () => {
            const filename = data.filename.replace('search-data', 'product-data');
            const dataPipeline = new DataPipeline(filename);
            await scrapeProductData(dataPipeline, url);
            await dataPipeline.closePipeline();
        })
    );

    await scrapeConcurrently(scrapeProductDataTasks, MAX_THREADS);

    logger.info('Scraping completed');
})();
