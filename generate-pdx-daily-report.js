require('dotenv').config();
const { ApiClient } = require('@mondaydotcomorg/api');
const { createClient } = require('redis');
const fs = require('fs');
const path = require('path');
const os = require('os');
const config = require('./pdx-daily-report.config');

const client = new ApiClient(process.env.MONDAY_API_TOKEN);
const SHARED_HISTORY_FILE = path.join(__dirname, 'history.json');
const HISTORY_FILE = path.join(__dirname, 'history-pdx-daily.json');
const HISTORY_SEED_FILE = path.join(__dirname, 'pdx-history-seed.json');
const LAST_RUN_FILE = path.join(__dirname, 'last_run_pdx_daily.txt');
const ACTIVITY_LOOKBACK_DAYS = [1, 2, 3, 4, 5];
const REDIS_HISTORY_KEY = process.env.REDIS_HISTORY_KEY || 'pdx:history';
const REDIS_LAST_RUN_KEY = process.env.REDIS_LAST_RUN_KEY || 'pdx:last_run';

async function generateReport() {
    let runtimeStore;
    try {
        runtimeStore = await createRuntimeStore();
        const forceRun = process.argv.some(arg => arg === '--force' || arg === '-f') || process.env.FORCE_RUN === '1';
        const now = new Date();
        const utcHour = now.getUTCHours();
        const todayKey = now.toISOString().split('T')[0];
        const isScheduledTime = utcHour === 19;

        if (forceRun) {
            console.log('Force mode enabled; bypassing schedule/duplicate safeguards.');
        }

        const lastRun = await runtimeStore.readLastRun();
        if (lastRun) {
            if (lastRun === todayKey && !forceRun) {
                console.log('PDX report already generated today. Skipping.');
                return;
            }
            if (lastRun === todayKey && forceRun) {
                console.log('Force mode: rerunning even though today\'s PDX report already exists.');
            }
        }

        if (!isScheduledTime && !forceRun) {
            console.log(`Not scheduled time (1pm CST / 19:00 UTC). Current UTC hour: ${utcHour}. Skipping.`);
            return;
        }
        if (!isScheduledTime && forceRun) {
            console.log(`Force mode: running outside scheduled window (UTC hour: ${utcHour}).`);
        }

        console.log(`Gathering ${config.REPORT_NAME} data for ${config.BOARD_NAME}...`);

        const [workspaceName, board] = await Promise.all([
            fetchWorkspaceName(),
            fetchTargetBoard()
        ]);

        const sharedHistory = readHistoryFile(SHARED_HISTORY_FILE);
        const pdxHistory = await runtimeStore.readPdxHistory();
        const historyForBoard = mergeBoardHistory(sharedHistory, pdxHistory, board.id);
        const comparison = resolveComparisonSnapshot(historyForBoard, todayKey, config.COMPARISON_DAYS);

        const boardItems = await fetchBoardItems(board);
        const summary = summarizeBoard(board, boardItems, comparison.data, historyForBoard, todayKey);
        const pdxHistoryToSave = {
            ...pdxHistory,
            [todayKey]: {
                ...(pdxHistory[todayKey] || {}),
                [board.id]: summary.boardStats.openItems
            }
        };
        pruneHistory(pdxHistoryToSave, config.HISTORY_RETENTION_DAYS);
        await runtimeStore.writePdxHistory(pdxHistoryToSave);

        const recentClosedItems = buildRecentClosedItems(boardItems, config.RECENT_CLOSED_DAYS);
        const closedCountsByDate = buildClosedCountsByDate(boardItems);
        const htmlContent = generateEmailHTML(workspaceName, board, summary.boardStats, recentClosedItems, closedCountsByDate);
        const csvContent = generateCSV(board, summary.boardStats, recentClosedItems);

        const outputFiles = saveOutputs(htmlContent, csvContent);
        await runtimeStore.writeLastRun(todayKey);

        console.log('Success. Output saved:');
        console.log(`  HTML (exports): ${outputFiles.exportHtml}`);
        if (outputFiles.oneDriveHtml) {
            console.log(`  HTML (OneDrive): ${outputFiles.oneDriveHtml}`);
        }
        console.log(`  CSV: ${outputFiles.csv}`);

        await sendEmailIfConfigured(htmlContent, csvContent, outputFiles.csvFileName);
    } catch (err) {
        console.error('Fatal Error:', err.message);
        if (err.stack) {
            console.error(err.stack);
        }
        process.exitCode = 1;
    } finally {
        if (runtimeStore) {
            await runtimeStore.close();
        }
    }
}

async function createRuntimeStore() {
    if (!process.env.REDIS_URL) {
        return createFileRuntimeStore();
    }

    const redisClient = createClient({ url: process.env.REDIS_URL });
    redisClient.on('error', error => {
        console.error(`Redis runtime store error: ${error.message}`);
    });

    await redisClient.connect();
    console.log(`Using Redis-backed runtime store for PDX history and last-run keys (${REDIS_HISTORY_KEY}, ${REDIS_LAST_RUN_KEY}).`);

    return {
        async readLastRun() {
            const value = await redisClient.get(REDIS_LAST_RUN_KEY);
            if (value) {
                return value.trim();
            }

            if (fs.existsSync(LAST_RUN_FILE)) {
                return fs.readFileSync(LAST_RUN_FILE, 'utf8').trim();
            }

            return '';
        },
        async writeLastRun(todayKey) {
            await redisClient.set(REDIS_LAST_RUN_KEY, todayKey);
        },
        async readPdxHistory() {
            const seedHistory = readHistoryFile(HISTORY_SEED_FILE);
            const value = await redisClient.get(REDIS_HISTORY_KEY);
            if (value) {
                try {
                    const redisHistory = JSON.parse(value);
                    return mergeDateStores(seedHistory, redisHistory);
                } catch (error) {
                    throw new Error(`Unable to parse Redis history key ${REDIS_HISTORY_KEY}: ${error.message}`);
                }
            }

            const fileHistory = readHistoryFile(HISTORY_FILE);
            const mergedSeedHistory = mergeDateStores(seedHistory, fileHistory);

            if (Object.keys(mergedSeedHistory).length > 0) {
                console.log(`Bootstrapping Redis history from bundled seed/file history into ${REDIS_HISTORY_KEY}.`);
                await redisClient.set(REDIS_HISTORY_KEY, JSON.stringify(mergedSeedHistory));
            }

            return mergedSeedHistory;
        },
        async writePdxHistory(historyStore) {
            await redisClient.set(REDIS_HISTORY_KEY, JSON.stringify(historyStore));
        },
        async close() {
            if (redisClient.isOpen) {
                await redisClient.quit();
            }
        }
    };
}

function createFileRuntimeStore() {
    return {
        async readLastRun() {
            if (!fs.existsSync(LAST_RUN_FILE)) {
                return '';
            }

            return fs.readFileSync(LAST_RUN_FILE, 'utf8').trim();
        },
        async writeLastRun(todayKey) {
            fs.writeFileSync(LAST_RUN_FILE, todayKey, 'utf8');
        },
        async readPdxHistory() {
            return readHistoryFile(HISTORY_FILE);
        },
        async writePdxHistory(historyStore) {
            fs.writeFileSync(HISTORY_FILE, JSON.stringify(historyStore, null, 2), 'utf8');
        },
        async close() {
            return undefined;
        }
    };
}

async function fetchWorkspaceName() {
    const workspaceRes = await client.query(`query { workspaces(ids: ${config.TARGET_WORKSPACE_ID}) { name } }`);
    return workspaceRes.workspaces[0]?.name || 'Monday Workspace';
}

async function fetchTargetBoard() {
    const boardsRes = await client.query(`query { boards(workspace_ids: ${config.TARGET_WORKSPACE_ID}, limit: 100) { id name state } }`);
    const board = boardsRes.boards.find(b => b.state === 'active' && b.name === config.BOARD_NAME);

    if (!board) {
        throw new Error(`Could not find active board named "${config.BOARD_NAME}".`);
    }

    return board;
}

async function fetchBoardItems(board) {
    const boardItems = [];
    const columnIds = [
        config.COL_IDS.STATUS,
        config.COL_IDS.PERSON,
        config.COL_IDS.FIELD_TECH,
        config.COL_IDS.CX_ALLOY,
        config.COL_IDS.UNIT,
        config.COL_IDS.ROMP,
        config.COL_IDS.PRIORITY,
        config.COL_IDS.DATE_CLOSED
    ];

    let cursor = null;
    let hasMore = true;

    while (hasMore) {
        const query = `query {
            boards(ids: ${board.id}) {
                items_page(cursor: ${cursor ? JSON.stringify(cursor) : 'null'}, limit: 100) {
                    cursor
                    items {
                        id
                        name
                        created_at
                        updated_at
                        state
                        group { title }
                        column_values(ids: ${JSON.stringify(columnIds)}) {
                            id
                            text
                            value
                        }
                    }
                }
            }
        }`;

        const res = await client.query(query);
        const page = res.boards[0]?.items_page;
        const excludedGroups = (config.EXCLUDED_GROUPS || []).map(group => group.toLowerCase());

        (page?.items || []).forEach(item => {
            if (item.state !== 'active') {
                return;
            }

            const groupTitle = item.group?.title || 'No Group';
            if (excludedGroups.some(excluded => groupTitle.toLowerCase().includes(excluded))) {
                return;
            }

            const cols = {};
            const colValues = {};

            (item.column_values || []).forEach(cv => {
                cols[cv.id] = cv.text || '';
                colValues[cv.id] = cv.value || '';
            });

            const assignedPerson = cols[config.COL_IDS.PERSON] || '';
            const fieldTech = cols[config.COL_IDS.FIELD_TECH] || '';
            const statusChangedAt = parseStatusChangedAt(colValues[config.COL_IDS.STATUS]);
            const reviewClosedAt = parseReviewClosedAt(cols[config.COL_IDS.DATE_CLOSED]);
            const closedEventAt = parseClosedEventAt(cols[config.COL_IDS.DATE_CLOSED], statusChangedAt);

            boardItems.push({
                id: item.id,
                name: item.name,
                created_at: item.created_at,
                updated_at: item.updated_at,
                board: board.name,
                boardId: board.id,
                group: groupTitle,
                status: cols[config.COL_IDS.STATUS] || '',
                assignedPerson: assignedPerson || 'Unassigned',
                fieldTech: fieldTech || '',
                assignedDisplay: assignedPerson || fieldTech || 'Unassigned',
                cxAlloy: cols[config.COL_IDS.CX_ALLOY] || '',
                unit: cols[config.COL_IDS.UNIT] || '',
                romp: cols[config.COL_IDS.ROMP] || '',
                priority: cols[config.COL_IDS.PRIORITY] || '',
                dateClosed: cols[config.COL_IDS.DATE_CLOSED] || '',
                statusValue: colValues[config.COL_IDS.STATUS] || '',
                statusChangedAt,
                reviewClosedAt,
                recentClosedAt: closedEventAt
            });
        });

        cursor = page?.cursor || null;
        hasMore = Boolean(cursor);
    }

    return boardItems;
}

function summarizeBoard(board, boardItems, comparisonData, historyStore, todayKey) {
    const now = new Date();
    const startOfToday = new Date(now);
    startOfToday.setHours(0, 0, 0, 0);

    const getDaysAgo = days => {
        const date = new Date(startOfToday);
        date.setDate(date.getDate() - days);
        return date;
    };

    const openItems = boardItems.filter(item => config.OPEN_STATUSES.includes(item.status));
    const openCount = openItems.length;

    const closedRecently = boardItems.filter(item =>
        config.CLOSED_STATUSES.includes(item.status) &&
        item.reviewClosedAt &&
        item.reviewClosedAt >= getDaysAgo(config.COMPARISON_DAYS)
    ).length;

    const newLast5 = boardItems.filter(item => new Date(item.created_at) >= getDaysAgo(5)).length;
    const newLast10 = boardItems.filter(item => new Date(item.created_at) >= getDaysAgo(10)).length;
    const newLast30 = boardItems.filter(item => new Date(item.created_at) >= getDaysAgo(30)).length;
    const newIssuesRecent = boardItems.filter(item => new Date(item.created_at) >= getDaysAgo(config.COMPARISON_DAYS)).length;

    let changeVal = 0;
    if (comparisonData && comparisonData[board.id] !== undefined) {
        changeVal = openCount - comparisonData[board.id];
    }

    let healthScore;
    if (openCount === 0 && newIssuesRecent === 0) {
        healthScore = 100;
    } else {
        const backlogPenalty = 1 / (1 + (openCount / config.OPEN_TARGET));
        const flowScore = (closedRecently + newIssuesRecent === 0)
            ? 1.0
            : closedRecently / (closedRecently + newIssuesRecent + 1);
        healthScore = Math.round(100 * (0.65 * backlogPenalty + 0.35 * flowScore));
    }

    let trend = 'stable';
    if (changeVal < -2) {
        trend = 'improving';
    } else if (changeVal > 2) {
        trend = 'declining';
    }

    const activityWindows = ACTIVITY_LOOKBACK_DAYS.map(days => {
        const snapshot = resolveComparisonSnapshot(historyStore || {}, todayKey, days, 1);
        const priorOpenCount = snapshot.data?.[board.id];
        const netChange = priorOpenCount !== undefined ? openCount - priorOpenCount : null;
        const closedCount = boardItems.filter(item =>
            config.CLOSED_STATUSES.includes(item.status) &&
            item.reviewClosedAt &&
            item.reviewClosedAt >= getDaysAgo(days)
        ).length;

        return {
            days,
            netChange,
            closedCount,
            comparisonDateUsed: snapshot.data ? snapshot.dateUsed : null
        };
    });

    const agingItems = { warning: [], critical: [], severe: [] };
    openItems.forEach(item => {
        const ageInDays = Math.floor((now - new Date(item.created_at)) / (24 * 60 * 60 * 1000));
        const agingItem = { ...item, ageInDays };

        if (ageInDays >= config.AGING_THRESHOLDS.SEVERE) {
            agingItems.severe.push(agingItem);
        } else if (ageInDays >= config.AGING_THRESHOLDS.CRITICAL) {
            agingItems.critical.push(agingItem);
        } else if (ageInDays >= config.AGING_THRESHOLDS.WARNING) {
            agingItems.warning.push(agingItem);
        }
    });

    return {
        boardStats: {
            id: board.id,
            name: board.name,
            totalItems: boardItems.length,
            newLast5,
            newLast10,
            newLast30,
            newIssuesRecent,
            openItems: openCount,
            closedRecently,
            change: changeVal,
            healthScore,
            trend,
            activityWindows
        },
        agingItems
    };
}

function buildRecentClosedItems(boardItems, lookbackDays) {
    const cutoff = new Date();
    cutoff.setHours(0, 0, 0, 0);
    cutoff.setDate(cutoff.getDate() - lookbackDays);
    const recentStatuses = new Set(config.RECENT_SECTION_STATUSES || []);

    return boardItems
        .filter(item =>
            recentStatuses.has(item.status) &&
            item.recentClosedAt &&
            item.recentClosedAt >= cutoff
        )
        .sort((a, b) =>
            (b.recentClosedAt - a.recentClosedAt) ||
            ((b.statusChangedAt?.getTime() || 0) - (a.statusChangedAt?.getTime() || 0)) ||
            (new Date(b.updated_at) - new Date(a.updated_at))
        );
}

function buildClosedCountsByDate(boardItems) {
    return boardItems.reduce((counts, item) => {
        if (!config.CLOSED_STATUSES.includes(item.status) || !item.reviewClosedAt) {
            return counts;
        }

        const dateKey = item.reviewClosedAt.toISOString().slice(0, 10);
        counts[dateKey] = (counts[dateKey] || 0) + 1;
        return counts;
    }, {});
}

function parseReviewClosedAt(dateClosedText) {
    if (!dateClosedText) {
        return null;
    }

    const closedDate = parseDateKey(dateClosedText);
    if (!closedDate) {
        return null;
    }

    return closedDate;
}

function parseStatusChangedAt(statusValue) {
    if (statusValue) {
        try {
            const parsed = JSON.parse(statusValue);
            if (parsed.changed_at) {
                const changedDate = new Date(parsed.changed_at);
                if (!Number.isNaN(changedDate.getTime())) {
                    return changedDate;
                }
            }
        } catch (err) {
            // Fall through to the manual date field.
        }
    }

    return null;
}

function parseClosedEventAt(dateClosedText, fallbackChangedAt) {
    if (dateClosedText) {
        const closedDate = parseDateKey(dateClosedText);
        if (closedDate) {
            return closedDate;
        }
    }

    return fallbackChangedAt || null;
}

function resolveComparisonSnapshot(historyStore, todayKey, comparisonDays, maxDistanceDays = 2) {
    const targetDate = new Date();
    targetDate.setDate(targetDate.getDate() - comparisonDays);
    const targetDateKey = targetDate.toISOString().split('T')[0];

    let comparisonData = historyStore[targetDateKey];
    let comparisonDateUsed = targetDateKey;

    if (!comparisonData) {
        const sortedDates = Object.keys(historyStore).filter(date => date < todayKey).sort();

        if (sortedDates.length > 0) {
            const targetTime = targetDate.getTime();
            let closestDate = sortedDates[0];
            let closestDiff = Math.abs(parseDateKey(closestDate).getTime() - targetTime);

            for (const dateKey of sortedDates) {
                const diff = Math.abs(parseDateKey(dateKey).getTime() - targetTime);
                if (diff < closestDiff) {
                    closestDiff = diff;
                    closestDate = dateKey;
                }
            }

            const daysDiff = closestDiff / (24 * 60 * 60 * 1000);
            if (daysDiff <= maxDistanceDays) {
                comparisonDateUsed = closestDate;
                comparisonData = historyStore[comparisonDateUsed];
            } else {
                comparisonData = null;
            }
        }
    }

    return {
        data: comparisonData,
        dateUsed: comparisonDateUsed
    };
}

function readHistoryFile(filePath) {
    if (!fs.existsSync(filePath)) {
        return {};
    }

    try {
        return JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch (err) {
        console.error(`Unable to read history file ${filePath}: ${err.message}`);
        return {};
    }
}

function mergeDateStores(primaryStore, secondaryStore) {
    const merged = {};
    const allDates = new Set([
        ...Object.keys(primaryStore || {}),
        ...Object.keys(secondaryStore || {})
    ]);

    [...allDates].sort().forEach(date => {
        merged[date] = {
            ...(primaryStore?.[date] || {}),
            ...(secondaryStore?.[date] || {})
        };
    });

    return merged;
}

function mergeBoardHistory(sharedHistory, localHistory, boardId) {
    const merged = {};
    const allDates = new Set([
        ...Object.keys(sharedHistory || {}),
        ...Object.keys(localHistory || {})
    ]);

    [...allDates].sort().forEach(date => {
        const sharedValue = sharedHistory?.[date]?.[boardId];
        const localValue = localHistory?.[date]?.[boardId];
        if (sharedValue !== undefined || localValue !== undefined) {
            merged[date] = {
                [boardId]: localValue !== undefined ? localValue : sharedValue
            };
        }
    });

    return merged;
}

function pruneHistory(historyStore, retentionDays) {
    const retentionDate = new Date();
    retentionDate.setDate(retentionDate.getDate() - retentionDays);

    Object.keys(historyStore).forEach(date => {
        if (parseDateKey(date) < retentionDate) {
            delete historyStore[date];
        }
    });
}

function saveOutputs(htmlContent, csvContent) {
    const exportsPath = path.join(__dirname, 'exports');
    if (!fs.existsSync(exportsPath)) {
        fs.mkdirSync(exportsPath, { recursive: true });
    }

    const onedrivePath = process.env.NODE_ENV !== 'production'
        ? path.join(os.homedir(), 'OneDrive - Power Solutions Int\'l', 'MondayReports')
        : null;

    if (onedrivePath && !fs.existsSync(onedrivePath)) {
        fs.mkdirSync(onedrivePath, { recursive: true });
    }

    const dateStr = new Date().toISOString().split('T')[0];
    const time = new Date();
    const timeStr = `${time.getHours().toString().padStart(2, '0')}${time.getMinutes().toString().padStart(2, '0')}`;

    const htmlFileName = `pdx-daily-report-${dateStr}-${timeStr}.html`;
    const csvFileName = `pdx-daily-report-data-${dateStr}-${timeStr}.csv`;

    const exportHtml = path.join(exportsPath, htmlFileName);
    const csv = path.join(exportsPath, csvFileName);

    fs.writeFileSync(exportHtml, htmlContent, 'utf8');
    fs.writeFileSync(csv, csvContent, 'utf8');

    let oneDriveHtml = null;
    if (onedrivePath) {
        oneDriveHtml = path.join(onedrivePath, htmlFileName);
        fs.writeFileSync(oneDriveHtml, htmlContent, 'utf8');
    }

    return { exportHtml, oneDriveHtml, csv, htmlFileName, csvFileName };
}

async function sendEmailIfConfigured(htmlContent, csvContent, csvFileName) {
    const recipientList = getReportRecipientValue();
    if (!recipientList) {
        return;
    }

    if (process.env.M365_TENANT_ID && process.env.M365_CLIENT_ID && process.env.M365_CLIENT_SECRET && process.env.M365_SENDER_UPN) {
        await sendEmailViaGraph(htmlContent, csvContent, csvFileName);
        return;
    }

    if (!process.env.EMAIL_USER || !process.env.EMAIL_PASS) {
        console.log('  Email skipped: no Graph app config and no SMTP credentials configured.');
        return;
    }

    const nodemailer = require('nodemailer');
    const transporter = nodemailer.createTransport({
        host: 'smtp.office365.com',
        port: 587,
        secure: false,
        auth: {
            user: process.env.EMAIL_USER,
            pass: process.env.EMAIL_PASS
        }
    });

    const today = new Date();
    const subjectDate = `${(today.getMonth() + 1).toString().padStart(2, '0')}/${today.getDate().toString().padStart(2, '0')}/${today.getFullYear()}`;

    await transporter.sendMail({
        from: `"Monday Report Bot" <${process.env.EMAIL_USER}>`,
        to: recipientList,
        subject: `PDX Daily Report - ${subjectDate}`,
        html: htmlContent,
        attachments: [{
            filename: csvFileName,
            content: csvContent
        }]
    });

    console.log(`  Email sent: ${recipientList}`);
}

async function sendEmailViaGraph(htmlContent, csvContent, csvFileName) {
    const tenantId = process.env.M365_TENANT_ID;
    const clientId = process.env.M365_CLIENT_ID;
    const clientSecret = process.env.M365_CLIENT_SECRET;
    const senderUpn = process.env.M365_SENDER_UPN;
    const tokenUrl = `https://login.microsoftonline.com/${encodeURIComponent(tenantId)}/oauth2/v2.0/token`;

    const tokenRes = await fetch(tokenUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
        },
        body: new URLSearchParams({
            client_id: clientId,
            client_secret: clientSecret,
            scope: 'https://graph.microsoft.com/.default',
            grant_type: 'client_credentials'
        })
    });

    if (!tokenRes.ok) {
        const body = await tokenRes.text();
        throw new Error(`Graph token request failed (${tokenRes.status}): ${body}`);
    }

    const tokenData = await tokenRes.json();
    const accessToken = tokenData.access_token;
    const today = new Date();
    const subjectDate = `${(today.getMonth() + 1).toString().padStart(2, '0')}/${today.getDate().toString().padStart(2, '0')}/${today.getFullYear()}`;
    const recipientValue = getReportRecipientValue();
    const recipients = splitRecipients(recipientValue).map(address => ({
        emailAddress: { address }
    }));

    const graphRes = await fetch(`https://graph.microsoft.com/v1.0/users/${encodeURIComponent(senderUpn)}/sendMail`, {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${accessToken}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            message: {
                subject: `PDX Daily Report - ${subjectDate}`,
                body: {
                    contentType: 'HTML',
                    content: htmlContent
                },
                toRecipients: recipients,
                attachments: [{
                    '@odata.type': '#microsoft.graph.fileAttachment',
                    name: csvFileName,
                    contentType: 'text/csv',
                    contentBytes: Buffer.from(csvContent, 'utf8').toString('base64')
                }]
            },
            saveToSentItems: true
        })
    });

    if (!graphRes.ok) {
        const body = await graphRes.text();
        throw new Error(`Graph sendMail failed (${graphRes.status}): ${body}`);
    }

    console.log(`  Email sent via Microsoft Graph: ${recipientValue}`);
}


function generateClosedByDateHTML(closedCountsByDate, lookbackDays = 30) {
    const cutoff = new Date();
    cutoff.setHours(0, 0, 0, 0);
    cutoff.setDate(cutoff.getDate() - lookbackDays);

    const dateCounts = [];
    const cursor = new Date(cutoff);
    const today = new Date();
    today.setHours(23, 59, 59, 999);

    while (cursor <= today) {
        const dateKey = formatDateKey(cursor);
        dateCounts.push({ date: dateKey, count: closedCountsByDate[dateKey] || 0 });
        cursor.setDate(cursor.getDate() + 1);
    }

    const totalClosed = dateCounts.reduce((sum, entry) => sum + entry.count, 0);
    const daysWithClosures = dateCounts.filter(entry => entry.count > 0).length;
    const peakDay = dateCounts.reduce((best, entry) => entry.count > best.count ? entry : best, { date: '', count: 0 });
    const avgPerDay = dateCounts.length > 0 ? (totalClosed / dateCounts.length).toFixed(1) : '0';

    if (totalClosed === 0) {
        return `
        <div style="border: 1px solid #e2e8f0; border-radius: 8px; padding: 18px; color: #64748b; font-size: 13px; background-color: #f8fafc;">
            No items with a Review/Closure Date in the last ${lookbackDays} days.
        </div>`;
    }

    const recentDays = dateCounts.slice(-10);
    const maxBarPixels = 200;
    const rows = recentDays.map((entry, index) => {
        const rowBg = index % 2 === 1 ? '#f8fafc' : '#ffffff';
        const countColor = entry.count > 0 ? '#2563eb' : '#94a3b8';
        const barPixels = peakDay.count > 0 ? Math.max(Math.round((entry.count / peakDay.count) * maxBarPixels), 0) : 0;
        const spacerPixels = maxBarPixels - barPixels;
        const barCell = entry.count > 0
            ? `<table cellpadding="0" cellspacing="0" border="0" width="${maxBarPixels}"><tr>
                    <td width="${barPixels}" height="14" bgcolor="#3b82f6" style="font-size: 1px; line-height: 1px;">&nbsp;</td>
                    ${spacerPixels > 0 ? `<td width="${spacerPixels}" style="font-size: 1px; line-height: 1px;">&nbsp;</td>` : ''}
               </tr></table>`
            : '';
        return `
            <tr style="background-color: ${rowBg};">
                <td style="padding: 10px 12px; border-top: 1px solid #e2e8f0; font-size: 12px; color: #0f172a; font-weight: 600;">${escapeHtml(formatDate(parseDateKey(entry.date)))}</td>
                <td align="center" style="padding: 10px 12px; border-top: 1px solid #e2e8f0; font-size: 13px; color: ${countColor}; font-weight: 800;">${entry.count}</td>
                <td style="padding: 10px 12px; border-top: 1px solid #e2e8f0;">${barCell}</td>
            </tr>`;
    }).join('');

    return `
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border: 1px solid #cbd5e1; border-collapse: collapse; border-radius: 8px; overflow: hidden; background-color: #ffffff;">
        <tr style="background-color: #0f172a; color: #ffffff;">
            <td colspan="3" style="padding: 12px 14px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                    <tr>
                        <td style="font-size: 14px; font-weight: 800; letter-spacing: 0.5px; text-transform: uppercase;">Closed Items by Date</td>
                        <td align="right" style="font-size: 12px; font-weight: 600;">Last 10 calendar days</td>
                    </tr>
                </table>
            </td>
        </tr>
        <tr style="background-color: #eff6ff;">
            <td style="padding: 12px 14px; border-top: 1px solid #cbd5e1;">
                <div style="font-size: 10px; font-weight: 800; color: #64748b; text-transform: uppercase; letter-spacing: 0.6px; margin-bottom: 4px;">Total (${lookbackDays}D)</div>
                <div style="font-size: 24px; font-weight: 900; color: #2563eb;">${totalClosed}</div>
            </td>
            <td style="padding: 12px 14px; border-top: 1px solid #cbd5e1; border-left: 1px solid #cbd5e1;">
                <div style="font-size: 10px; font-weight: 800; color: #64748b; text-transform: uppercase; letter-spacing: 0.6px; margin-bottom: 4px;">Avg / Day</div>
                <div style="font-size: 24px; font-weight: 900; color: #0f172a;">${avgPerDay}</div>
            </td>
            <td style="padding: 12px 14px; border-top: 1px solid #cbd5e1; border-left: 1px solid #cbd5e1;">
                <div style="font-size: 10px; font-weight: 800; color: #64748b; text-transform: uppercase; letter-spacing: 0.6px; margin-bottom: 4px;">Days w/ Closures</div>
                <div style="font-size: 24px; font-weight: 900; color: #10b981;">${daysWithClosures} <span style="font-size: 12px; color: #94a3b8;">/ ${lookbackDays}</span></div>
            </td>
        </tr>
        <tr style="background-color: #f8fafc;">
            <th align="left" style="padding: 10px 12px; border-top: 1px solid #e2e8f0; font-size: 11px; color: #475569; text-transform: uppercase; letter-spacing: 0.5px;">Date</th>
            <th align="center" style="padding: 10px 12px; border-top: 1px solid #e2e8f0; font-size: 11px; color: #475569; text-transform: uppercase; letter-spacing: 0.5px;">Closed</th>
            <th align="left" style="padding: 10px 12px; border-top: 1px solid #e2e8f0; font-size: 11px; color: #475569; text-transform: uppercase; letter-spacing: 0.5px;"></th>
        </tr>
        ${rows}
    </table>`;
}

function generateCSV(board, boardStats, recentClosedItems) {
    const lines = [];

    lines.push('BOARD SUMMARY');
    lines.push(`Board Name,New (5D),New (10D),New (30D),Open Items,Closed (${config.COMPARISON_DAYS}D),Change,Trend,Total Items`);
    lines.push([
        csvValue(boardStats.name),
        boardStats.newLast5,
        boardStats.newLast10,
        boardStats.newLast30,
        boardStats.openItems,
        boardStats.closedRecently,
        boardStats.change,
        csvValue(boardStats.trend),
        boardStats.totalItems
    ].join(','));

    lines.push('');
    lines.push('BOARD ACTIVITY SUMMARY');
    lines.push('Metric,1D,2D,3D,4D,5D');
    lines.push([
        csvValue('Closed'),
        ...boardStats.activityWindows.map(window => window.closedCount)
    ].join(','));

    lines.push('');
    lines.push(`RECENT CLOSED REQUESTS (Last ${config.RECENT_CLOSED_DAYS} Days)`);
    lines.push('Closed Date,Status,CX Alloy,Item Name,Group,Unit,ROMP,Priority,Assigned');
    recentClosedItems.forEach(item => {
        lines.push([
            csvValue(formatDate(item.recentClosedAt)),
            csvValue(item.status),
            csvValue(item.cxAlloy),
            csvValue(item.name),
            csvValue(item.group),
            csvValue(item.unit),
            csvValue(item.romp),
            csvValue(item.priority),
            csvValue(item.assignedDisplay)
        ].join(','));
    });

    return lines.join('\n');
}

function generateEmailHTML(workspaceName, board, boardStats, recentClosedItems, closedCountsByDate) {
    const today = new Date().toLocaleDateString('en-US', {
        weekday: 'long',
        year: 'numeric',
        month: 'long',
        day: 'numeric'
    });
    const closedByDateHTML = generateClosedByDateHTML(closedCountsByDate, 30);

    const activityHeaderCells = boardStats.activityWindows.map(window => `
                                    <th style="padding: 14px 10px; text-align: center; font-size: 14px; font-weight: 800; letter-spacing: 0.5px;">${window.days}D</th>`).join('');
    const activityClosedCells = boardStats.activityWindows.map(window => `
                                    <td align="center" style="padding: 18px 10px; background-color: #eff6ff; border-left: 1px solid #e2e8f0;">
                                        <div style="font-size: 28px; font-weight: 900; color: #2563eb; line-height: 1;">${window.closedCount}</div>
                                    </td>`).join('');

    const recentItemsMarkup = recentClosedItems.length === 0
        ? `<tr>
                <td style="padding: 24px; font-size: 14px; color: #64748b;">No items moved into a closed-status group during the last ${config.RECENT_CLOSED_DAYS} days.</td>
           </tr>`
        : recentClosedItems.map((item, idx) => {
            const itemUrl = `https://${config.MONDAY_SLUG}.monday.com/boards/${item.boardId}/pulses/${item.id}`;
            const closedDateLabel = formatDate(item.recentClosedAt);
            return `
                <tr style="${idx < recentClosedItems.length - 1 ? 'border-bottom: 1px solid #eeeeee;' : ''}">
                    <td style="padding: 20px;">
                        <table width="100%" cellpadding="0" cellspacing="0" border="0">
                            <tr>
                                <td width="120" style="font-size: 12px; color: #333333; vertical-align: top; font-weight: bold; padding-top: 5px;">
                                    <span style="display: block; font-size: 10px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px;">Closure Date</span>
                                    <span style="display: block; font-size: 14px; color: #111111;">${escapeHtml(closedDateLabel)}</span>
                                </td>
                                <td style="vertical-align: top;">
                                    <div style="margin-bottom: 12px;">
                                        ${item.cxAlloy ? `
                                        <table align="left" cellpadding="0" cellspacing="0" style="margin-right: 15px; margin-bottom: 5px;">
                                            <tr><td style="border: 2px solid #333333; padding: 2px 8px;">
                                                <a href="${itemUrl}" target="_blank" style="color: #111111; font-size: 11px; font-weight: 900; text-decoration: none;">${escapeHtml(item.cxAlloy)}</a>
                                            </td></tr>
                                        </table>` : ''}
                                        <a href="${itemUrl}" target="_blank" style="font-weight: bold; color: #111111; font-size: 16px; text-decoration: none;">${escapeHtml(item.name)}</a>${renderPriorityBadge(item.priority)}
                                    </div>
                                    <table cellpadding="0" cellspacing="0" border="0" style="font-size: 13px; color: #444444; line-height: 1.8;">
                                        <tr><td width="95" style="color: #0f766e; font-weight: bold; padding-right: 10px;">Closure Date:</td><td>${escapeHtml(closedDateLabel)}</td></tr>
                                        <tr><td width="95" style="color: #0f766e; font-weight: bold; padding-right: 10px;">Group:</td><td>${escapeHtml(item.group)}</td></tr>
                                        ${item.unit ? `<tr><td style="color: #0f766e; font-weight: bold; padding-right: 10px;">Unit:</td><td>${escapeHtml(item.unit)}</td></tr>` : ''}
                                        ${item.romp ? `<tr><td style="color: #0f766e; font-weight: bold; padding-right: 10px;">Romp:</td><td>${escapeHtml(item.romp)}</td></tr>` : ''}
                                    </table>
                                </td>
                                <td align="right" width="220" style="vertical-align: top; padding-top: 5px;">
                                    ${renderStatusBadge(item.status)}
                                    <div style="font-size: 12px; color: #777777; margin-top: 10px;">
                                        <span style="color: #0f766e; font-weight: bold;">Assigned:</span><br>
                                        <span style="color: #111111; font-weight: bold; font-size: 13px;">${escapeHtml(item.assignedDisplay)}</span>
                                    </div>
                                    ${item.fieldTech && item.fieldTech !== item.assignedDisplay ? `
                                    <div style="font-size: 11px; color: #777777; margin-top: 6px;">
                                        <span style="color: #0f766e; font-weight: bold;">Field Tech:</span> ${escapeHtml(item.fieldTech)}
                                    </div>` : ''}
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>`;
        }).join('');

    return `<!DOCTYPE html>
<html>
<head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta charset="utf-8">
</head>
<body style="margin: 0; padding: 0; font-family: Segoe UI, Arial, sans-serif; background-color: #f5f5f5;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #f5f5f5; padding: 15px;">
        <tr>
            <td align="center">
                <table width="95%" cellpadding="0" cellspacing="0" border="0" style="max-width: 1280px; background-color: #ffffff; border: 1px solid #dddddd; width: 95% !important;">
                    <tr>
                        <td style="background: linear-gradient(135deg, #0f766e 0%, #0ea5e9 100%); background-color: #0f766e; padding: 28px 35px; text-align: center; color: #ffffff; border-radius: 16px 16px 0 0;">
                            <h1 style="margin: 0; font-size: 28px; font-weight: bold; letter-spacing: -0.5px;">${escapeHtml(config.REPORT_NAME)}</h1>
                            <p style="margin: 8px 0 0 0; font-size: 13px; opacity: 0.95;">${escapeHtml(board.name)} | ${escapeHtml(workspaceName)} | ${today}</p>
                        </td>
                    </tr>

                    <tr>
                        <td style="padding: 20px 25px 8px 25px; background-color: #f8fafc;">
                            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    <td style="padding-bottom: 12px;">
                                        <table cellpadding="0" cellspacing="0" border="0" width="100%">
                                            <tr>
                                                <td style="font-size: 24px; color: #f59e0b; vertical-align: middle;">&#9889;</td>
                                                <td style="font-size: 15px; font-weight: bold; color: #1e293b; text-transform: uppercase; letter-spacing: 1.5px; padding-left: 10px; vertical-align: middle;">Velocity Metrics</td>
                                                <td style="width: 100%; padding-left: 15px; vertical-align: middle;"><div style="height: 2px; background: linear-gradient(90deg, #e2e8f0 0%, transparent 100%); background-color: #e2e8f0;"></div></td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        <table width="100%" cellpadding="0" cellspacing="10" border="0">
                                            <tr>
                                                <td width="33.33%" style="background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 15px 18px; position: relative;">
                                                    <div style="font-size: 10px; font-weight: bold; color: #64748b; text-transform: uppercase; margin-bottom: 3px;">New (5 Days)</div>
                                                    <div style="font-size: 32px; font-weight: bold; color: #10b981;">${boardStats.newLast5}</div>
                                                    <div style="position: absolute; bottom: 3px; right: 8px; font-size: 44px; color: #6ee7b7;">&#10010;</div>
                                                </td>
                                                <td width="33.33%" style="background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 15px 18px; position: relative;">
                                                    <div style="font-size: 10px; font-weight: bold; color: #64748b; text-transform: uppercase; margin-bottom: 3px;">New (10 Days)</div>
                                                    <div style="font-size: 32px; font-weight: bold; color: #10b981;">${boardStats.newLast10}</div>
                                                    <div style="position: absolute; bottom: 3px; right: 8px; font-size: 44px; color: #6ee7b7;">&#8635;</div>
                                                </td>
                                                <td width="33.33%" style="background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 15px 18px; position: relative;">
                                                    <div style="font-size: 10px; font-weight: bold; color: #64748b; text-transform: uppercase; margin-bottom: 3px;">New (30 Days)</div>
                                                    <div style="font-size: 32px; font-weight: bold; color: #10b981;">${boardStats.newLast30}</div>
                                                    <div style="position: absolute; bottom: 3px; right: 8px; font-size: 44px; color: #6ee7b7;">&#128197;</div>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                    <tr>
                        <td style="padding: 8px 25px 20px 25px; background-color: #f8fafc;">
                            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    <td style="padding-bottom: 12px;">
                                        <table cellpadding="0" cellspacing="0" border="0" width="100%">
                                            <tr>
                                                <td style="font-size: 24px; color: #10b981; vertical-align: middle;">&#9745;</td>
                                                <td style="font-size: 15px; font-weight: bold; color: #1e293b; text-transform: uppercase; letter-spacing: 1.5px; padding-left: 10px; vertical-align: middle;">Status &amp; Health</td>
                                                <td style="width: 100%; padding-left: 15px; vertical-align: middle;"><div style="height: 2px; background: linear-gradient(90deg, #e2e8f0 0%, transparent 100%); background-color: #e2e8f0;"></div></td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        <table width="100%" cellpadding="0" cellspacing="8" border="0">
                                            <tr>
                                                <td width="50%" style="background: #ffffff; border: 1px solid #e2e8f0; border-left: 4px solid #f43f5e; border-radius: 8px; padding: 12px; position: relative;">
                                                    <div style="font-size: 9px; font-weight: bold; color: #94a3b8; text-transform: uppercase;">Total Open</div>
                                                    <div style="font-size: 24px; font-weight: bold; color: #e11d48;">${boardStats.openItems}</div>
                                                    <div style="position: absolute; top: 5px; right: 8px; font-size: 32px; color: #fda4af;">&#128194;</div>
                                                </td>
                                                <td width="50%" style="background: #ffffff; border: 1px solid #e2e8f0; border-left: 4px solid #3b82f6; border-radius: 8px; padding: 12px; position: relative;">
                                                    <div style="font-size: 9px; font-weight: bold; color: #94a3b8; text-transform: uppercase;">Closed (${config.COMPARISON_DAYS}D)</div>
                                                    <div style="font-size: 24px; font-weight: bold; color: #2563eb;">${boardStats.closedRecently}</div>
                                                    <div style="position: absolute; top: 5px; right: 8px; font-size: 32px; color: #93c5fd;">&#9989;</div>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                    <tr>
                        <td style="padding: 26px 25px; background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%); background-color: #ffffff; border-top: 1px solid #e2e8f0; border-bottom: 1px solid #e2e8f0;">
                            <table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom: 12px;">
                                <tr>
                                    <td style="font-size: 22px; color: #10b981; vertical-align: middle;">&#9776;</td>
                                    <td style="font-size: 18px; font-weight: bold; color: #1e293b; text-transform: uppercase; letter-spacing: 1.5px; padding-left: 10px; vertical-align: middle;">Board Activity Summary</td>
                                    <td align="right" style="font-size: 12px; color: #475569; font-weight: 600;">${escapeHtml(boardStats.name)}</td>
                                </tr>
                            </table>
                            <p style="font-size: 12px; color: #64748b; margin: 0 0 16px 0; line-height: 1.6;">
                                Closed shows how many items moved into a closed status over each lookback window (1 to 5 calendar days).
                            </p>
                            <table width="100%" cellpadding="0" cellspacing="0" style="border: 2px solid #cbd5e1; border-collapse: collapse; border-radius: 12px; overflow: hidden; background-color: #ffffff;">
                                <tr style="background-color: #0f172a; color: #ffffff;">
                                    <th align="left" style="padding: 16px 14px; min-width: 170px; font-size: 13px; font-weight: 800; text-transform: uppercase; letter-spacing: 0.7px;">Metric</th>
                                    ${activityHeaderCells}
                                </tr>
                                <tr style="border-top: 1px solid #e2e8f0;">
                                    <td style="padding: 16px 14px; background-color: #eff6ff; font-size: 13px; font-weight: 800; color: #1d4ed8; text-transform: uppercase; letter-spacing: 0.7px;">Closed</td>
                                    ${activityClosedCells}
                                </tr>
                            </table>
                            <table width="100%" cellpadding="0" cellspacing="10" border="0" style="margin-top: 14px;">
                                <tr>
                                    <td width="50%" style="background: #ffffff; border: 1px solid #dbeafe; border-radius: 10px; padding: 14px 16px;">
                                        <div style="font-size: 11px; font-weight: 800; color: #64748b; text-transform: uppercase; letter-spacing: 0.6px; margin-bottom: 4px;">Current Open</div>
                                        <div style="font-size: 28px; font-weight: 900; color: #0f172a;">${boardStats.openItems}</div>
                                    </td>
                                    <td width="50%" style="background: #ffffff; border: 1px solid #dbeafe; border-radius: 10px; padding: 14px 16px;">
                                        <div style="font-size: 11px; font-weight: 800; color: #64748b; text-transform: uppercase; letter-spacing: 0.6px; margin-bottom: 4px;">Closed (30D)</div>
                                        <div style="font-size: 28px; font-weight: 900; color: #2563eb;">${Object.entries(closedCountsByDate).filter(([date]) => parseDateKey(date) >= new Date(new Date().setDate(new Date().getDate() - 30))).reduce((sum, [, count]) => sum + count, 0)}</div>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                    <tr>
                        <td style="padding: 20px 25px; background-color: #ffffff; border-top: 1px solid #e2e8f0;">
                            <table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom: 12px;">
                                <tr>
                                    <td style="font-size: 24px; color: #3b82f6; vertical-align: middle;">&#128200;</td>
                                    <td style="font-size: 15px; font-weight: bold; color: #1e293b; text-transform: uppercase; letter-spacing: 1.5px; padding-left: 10px; vertical-align: middle;">Closed Items by Date</td>
                                    <td style="width: 100%; padding-left: 15px; vertical-align: middle;"><div style="height: 2px; background: linear-gradient(90deg, #e2e8f0 0%, transparent 100%); background-color: #e2e8f0;"></div></td>
                                </tr>
                            </table>
                            <p style="font-size: 11px; color: #64748b; margin: 0 0 12px 0;">Closure counts are based on the Review/Closure Date column. Each row shows how many items were closed on that date.</p>
                            ${closedByDateHTML}
                        </td>
                    </tr>

                    <tr>
                        <td style="padding: 25px; background-color: #f9f9f9;">
                            <h2 style="font-size: 20px; margin-bottom: 20px; color: #333333;">Last ${config.RECENT_CLOSED_DAYS} Days - Requests Updated To Closed Statuses [Closed, Pending, or CX Alloy Update]</h2>
                            <table width="100%" cellpadding="0" cellspacing="0" style="background: #ffffff; border: 1px solid #dddddd; border-collapse: collapse;">
                                <tr>
                                    <td style="background-color: #2d3748; color: #ffffff; padding: 15px 20px;">
                                        <table width="100%" cellpadding="0" cellspacing="0">
                                            <tr>
                                                <td style="font-weight: bold; font-size: 16px; color: #ffffff;">${escapeHtml(board.name)}</td>
                                                <td align="right">
                                                    <span style="background-color: #4a5568; color: #ffffff; padding: 4px 10px; font-size: 11px; font-weight: bold; border-radius: 3px; margin-right: 8px;">UPDATED: ${recentClosedItems.length}</span>
                                                    <span style="background-color: #ffffff; color: #dc2626; padding: 4px 10px; font-size: 11px; font-weight: bold; border-radius: 3px; border: 1px solid #dc2626;">OPEN: ${boardStats.openItems}</span>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                ${recentItemsMarkup}
                            </table>
                        </td>
                    </tr>

                    <tr>
                        <td style="background-color: #0f172a; padding: 25px;">
                            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    <td style="color: #64748b; font-size: 11px;">
                                        <span style="display: inline-block; margin-right: 15px;">
                                            <span style="display: inline-block; width: 8px; height: 8px; border-radius: 50%; background-color: #10b981; margin-right: 5px; vertical-align: middle;"></span>
                                            <span style="vertical-align: middle;">System Online</span>
                                        </span>
                                        <span style="display: inline-block;">
                                            <span style="display: inline-block; width: 8px; height: 8px; border-radius: 50%; background-color: #3b82f6; margin-right: 5px; vertical-align: middle;"></span>
                                            <span style="vertical-align: middle;">Generated: ${new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}</span>
                                        </span>
                                    </td>
                                    <td align="right" style="color: #64748b; font-size: 10px;">
                                        ${escapeHtml(config.REPORT_NAME)} | ${config.COMPARISON_DAYS}-Day Comparison Window
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>`;
}

function renderAgingTable(title, headerColor, headingBg, headingTextColor, items, cxRenderer, altRowColor) {
    if (!items || items.length === 0) {
        return '';
    }

    return `<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 20px;">
        <tr>
            <td style="background-color: ${headerColor}; padding: 12px 15px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                    <tr>
                        <td style="color: #ffffff; font-weight: bold; font-size: 14px;">${title}</td>
                        <td align="right" style="color: #ffffff; font-weight: bold; font-size: 14px;">${items.length} items</td>
                    </tr>
                </table>
            </td>
        </tr>
        <tr>
            <td style="background-color: #ffffff; border: 2px solid ${headerColor}; border-top: none;">
                <table width="100%" cellpadding="10" cellspacing="0" border="0" style="border-collapse: collapse;">
                    <tr style="background-color: ${headingBg};">
                        <th align="center" width="60" style="font-size: 11px; color: ${headingTextColor}; border-bottom: 1px solid ${headingBg};">DAYS</th>
                        <th align="left" width="12%" style="font-size: 11px; color: ${headingTextColor}; border-bottom: 1px solid ${headingBg};">CX ALLOY</th>
                        <th align="left" style="font-size: 11px; color: ${headingTextColor}; border-bottom: 1px solid ${headingBg};">ITEM</th>
                        <th align="left" width="20%" style="font-size: 11px; color: ${headingTextColor}; border-bottom: 1px solid ${headingBg};">GROUP</th>
                        <th align="left" width="15%" style="font-size: 11px; color: ${headingTextColor}; border-bottom: 1px solid ${headingBg};">ASSIGNED</th>
                    </tr>
                    ${items.slice(0, 10).map((item, index) => `
                    <tr style="font-size: 12px; background-color: ${index % 2 === 0 ? '#ffffff' : `#${altRowColor}`};">
                        <td align="center" style="font-weight: bold; color: ${headerColor}; border-bottom: 1px solid ${headingBg};">${item.ageInDays}</td>
                        <td style="border-bottom: 1px solid ${headingBg};">${cxRenderer(item)}</td>
                        <td style="border-bottom: 1px solid ${headingBg};"><a href="https://${config.MONDAY_SLUG}.monday.com/boards/${item.boardId}/pulses/${item.id}" style="color: #111111; text-decoration: none;">${escapeHtml(item.name)}</a></td>
                        <td style="color: #475569; border-bottom: 1px solid ${headingBg};">${escapeHtml(item.group)}</td>
                        <td style="color: #475569; border-bottom: 1px solid ${headingBg};">${escapeHtml(item.assignedDisplay)}</td>
                    </tr>`).join('')}
                </table>
            </td>
        </tr>
    </table>`;
}


function parseDateKey(dateText) {
    const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(dateText || '');
    if (!match) {
        const parsed = new Date(dateText);
        return Number.isNaN(parsed.getTime()) ? null : parsed;
    }

    const [, year, month, day] = match;
    return new Date(Number(year), Number(month) - 1, Number(day), 12, 0, 0, 0);
}

function formatDateKey(date) {
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

function splitRecipients(value) {
    return String(value || '')
        .split(/[;,]/)
        .map(entry => entry.trim())
        .filter(Boolean);
}

function getReportRecipientValue() {
    return process.env.REPORT_TO_EMAIL || process.env.EMAIL_TO || '';
}

function renderStatusBadge(status) {
    const normalized = (status || '').toLowerCase();
    let bg = '#eff6ff';
    let border = '#bfdbfe';
    let text = '#1d4ed8';

    if (normalized.includes('closed') || normalized.includes('complete') || normalized.includes('done') || normalized.includes('resolved')) {
        bg = '#ecfdf5';
        border = '#a7f3d0';
        text = '#047857';
    } else if (normalized.includes('pending')) {
        bg = '#eff6ff';
        border = '#93c5fd';
        text = '#1d4ed8';
    } else if (normalized.includes('cx alloy')) {
        bg = '#ecfdf5';
        border = '#6ee7b7';
        text = '#065f46';
    } else if (normalized.includes('cancel')) {
        bg = '#faf5ff';
        border = '#d8b4fe';
        text = '#7e22ce';
    } else if (normalized.includes('non-issue')) {
        bg = '#eff6ff';
        border = '#93c5fd';
        text = '#1d4ed8';
    }

    return `<div style="background-color: ${bg}; border: 1px solid ${border}; padding: 5px 12px; display: inline-block; margin-bottom: 10px;">
        <span style="font-size: 10px; font-weight: bold; color: ${text}; text-transform: uppercase;">${escapeHtml(status || 'Unknown')}</span>
    </div>`;
}

function renderPriorityBadge(priority) {
    if (!priority) {
        return '';
    }

    const normalized = priority.toLowerCase();
    let bg = '#e5e7eb';
    let text = '#111827';

    if (normalized.includes('critical') || normalized.includes('nuclear')) {
        bg = '#dc2626';
        text = '#ffffff';
    } else if (normalized.includes('high')) {
        bg = '#f97316';
        text = '#ffffff';
    } else if (normalized.includes('moderate') || normalized.includes('medium')) {
        bg = '#eab308';
        text = '#111827';
    } else if (normalized.includes('low') || normalized.includes('observation')) {
        bg = '#64748b';
        text = '#ffffff';
    }

    return `<span style="background-color: ${bg}; color: ${text}; padding: 2px 6px; font-size: 9px; font-weight: bold; border-radius: 2px; margin-left: 8px;">${escapeHtml(priority.toUpperCase())}</span>`;
}

function formatMonthDay(date) {
    if (!date) {
        return '';
    }
    const workingDate = new Date(date);
    return `${(workingDate.getMonth() + 1).toString().padStart(2, '0')}/${workingDate.getDate().toString().padStart(2, '0')}`;
}

function formatDate(date) {
    if (!date) {
        return '';
    }
    const workingDate = new Date(date);
    return `${workingDate.getMonth() + 1}/${workingDate.getDate()}/${workingDate.getFullYear()}`;
}

function formatDateTime(date) {
    if (!date) {
        return '';
    }
    return new Date(date).toLocaleString('en-US', {
        month: 'numeric',
        day: 'numeric',
        year: 'numeric',
        hour: 'numeric',
        minute: '2-digit'
    });
}

function csvValue(value) {
    const text = value === undefined || value === null ? '' : String(value);
    return `"${text.replace(/"/g, '""')}"`;
}

function escapeHtml(value) {
    return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

generateReport().catch(err => {
    console.error('Fatal Error:', err.message);
    process.exitCode = 1;
});
