require('dotenv').config();
const { ApiClient } = require('@mondaydotcomorg/api');
const fs = require('fs');
const path = require('path');
const os = require('os');
const config = require('./config');

const client = new ApiClient(process.env.MONDAY_API_TOKEN);
const HISTORY_FILE = path.join(__dirname, 'history.json');
const LAST_RUN_FILE = path.join(__dirname, 'last_run.txt');

async function generateReport() {
    try {
        // Force flag allows manual runs outside schedule or repeat runs the same day
        const forceRun = process.argv.some(arg => arg === '--force' || arg === '-f') || process.env.FORCE_RUN === '1';

        // Check if already run today and if it's around 1pm CST (19:00 UTC)
        const now = new Date();
        const utcHour = now.getUTCHours();
        const todayKey = now.toISOString().split('T')[0];

        // 1pm CST = 19:00 UTC
        const isScheduledTime = utcHour === 19;

        if (forceRun) {
            console.log('⚡ Force mode enabled; bypassing schedule/duplicate safeguards.');
        }

        if (fs.existsSync(LAST_RUN_FILE)) {
            const lastRun = fs.readFileSync(LAST_RUN_FILE, 'utf8').trim();
            if (lastRun === todayKey && !forceRun) {
                console.log('📧 Report already generated today. Skipping.');
                return;
            } else if (lastRun === todayKey && forceRun) {
                console.log('⚡ Force mode: rerunning even though today\'s report already exists.');
            }
        }

        if (!isScheduledTime && !forceRun) {
            console.log(`📧 Not scheduled time (1pm CST / 19:00 UTC). Current UTC hour: ${utcHour}. Skipping.`);
            return;
        } else if (!isScheduledTime && forceRun) {
            console.log(`⚡ Force mode: running outside scheduled window (UTC hour: ${utcHour}).`);
        }

        console.log('📧 Gathering Enclosure Support Data...\n');

        const todayKey2 = new Date().toISOString().split('T')[0];
        let historyStore = {};

        if (fs.existsSync(HISTORY_FILE)) {
            try {
                historyStore = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8'));
            } catch (err) {
                console.error('   ⚠️ Error reading history file:', err.message);
            }
        }

        // Calculate comparison date based on config
        // Find data from ~COMPARISON_DAYS ago (closest available date within ±2 days)
        const targetDate = new Date();
        targetDate.setDate(targetDate.getDate() - config.COMPARISON_DAYS);
        const targetDateKey = targetDate.toISOString().split('T')[0];

        let comparisonData = historyStore[targetDateKey];
        let comparisonDateUsed = targetDateKey;

        if (!comparisonData) {
            // Find the closest date to target within acceptable range (5-9 days ago for 7-day config)
            const sortedDates = Object.keys(historyStore).filter(d => d < todayKey).sort();

            if (sortedDates.length > 0) {
                // Find closest date to target
                const targetTime = targetDate.getTime();
                let closestDate = sortedDates[0];
                let closestDiff = Math.abs(new Date(closestDate).getTime() - targetTime);

                for (const dateKey of sortedDates) {
                    const diff = Math.abs(new Date(dateKey).getTime() - targetTime);
                    if (diff < closestDiff) {
                        closestDiff = diff;
                        closestDate = dateKey;
                    }
                }

                // Only use if within ±2 days of target
                const daysDiff = closestDiff / (24 * 60 * 60 * 1000);
                if (daysDiff <= 2) {
                    comparisonDateUsed = closestDate;
                    comparisonData = historyStore[comparisonDateUsed];
                    console.log(`   ℹ️ Using closest comparison date: ${comparisonDateUsed}`);
                } else {
                    // No data within acceptable range - use null (will show 0 change)
                    comparisonData = null;
                    console.log(`   ⚠️ No ${config.COMPARISON_DAYS}-day data available (closest is ${Math.round(daysDiff)} days off)`);
                }
            }
        } else {
            console.log(`   ✅ Exact ${config.COMPARISON_DAYS}-day comparison found: ${targetDateKey}`);
        }

        const workspaceRes = await client.query(`query { workspaces(ids: ${config.TARGET_WORKSPACE_ID}) { name } }`);
        const workspaceName = workspaceRes.workspaces[0].name;

        const boardsRes = await client.query(`query { boards(workspace_ids: ${config.TARGET_WORKSPACE_ID}, limit: 100) { id name state } }`);

        const excludedByName = boardsRes.boards.filter(b =>
            b.state === 'active' &&
            !b.name.startsWith('Subitems of') &&
            config.EXCLUDED_BOARDS.includes(b.name)
        );
        console.log(`   🚫 Excluded boards: ${excludedByName.map(b => b.name).join(', ') || 'none'}`);

        const activeBoards = boardsRes.boards.filter(b =>
            b.state === 'active' &&
            !config.EXCLUDED_BOARDS.includes(b.name) &&
            !b.name.startsWith('Subitems of')
        );
        console.log(`   ✅ Processing ${activeBoards.length} boards: ${activeBoards.map(b => b.name).join(', ')}`);

        const allItems = [];
        const boardStats = [];
        const currentRunCounts = {};
        const agingItems = { warning: [], critical: [], severe: [] };

        await Promise.all(activeBoards.map(async (board) => {
            let boardItems = [];
            let cursor = null;
            let hasMore = true;

            try {
                while (hasMore) {
                    let query = `query {
                        boards(ids: ${board.id}) {
                            items_page(cursor: ${cursor ? `"${cursor}"` : "null"}, limit: 100) {
                                cursor
                                items {
                                    id name created_at updated_at state group { title }
                                    column_values(ids: ["${config.COL_IDS.STATUS}", "${config.COL_IDS.PERSON}", "${config.COL_IDS.CX_ALLOY}", "${config.COL_IDS.UNIT}", "${config.COL_IDS.ROMP}", "${config.COL_IDS.PRIORITY}", "${config.COL_IDS.DATE_CLOSED}"]) {
                                        id text value
                                    }
                                }
                            }
                        }
                    }`;

                    const res = await client.query(query);
                    const boardData = res.boards[0];

                    // Filter out archived/deleted items and excluded groups
                    const excludedGroups = (config.EXCLUDED_GROUPS || []).map(g => g.toLowerCase());
                    const activeItems = boardData.items_page.items.filter(item => {
                        if (item.state !== 'active') return false;
                        const groupTitle = (item.group?.title || '').toLowerCase();
                        return !excludedGroups.some(eg => groupTitle.includes(eg));
                    });
                    activeItems.forEach(item => {
                        const cols = {};
                        const colValues = {};
                        (item.column_values || []).forEach(cv => {
                            cols[cv.id] = cv.text;
                            colValues[cv.id] = cv.value;
                        });

                        const processed = {
                            id: item.id,
                            name: item.name,
                            created_at: item.created_at,
                            updated_at: item.updated_at,
                            board: board.name,
                            boardId: board.id,
                            group: item.group?.title || 'No Group',
                            status: cols[config.COL_IDS.STATUS] || '',
                            person: cols[config.COL_IDS.PERSON] || 'Unassigned',
                            cxAlloy: cols[config.COL_IDS.CX_ALLOY] || '',
                            unit: cols[config.COL_IDS.UNIT] || '',
                            romp: cols[config.COL_IDS.ROMP] || '',
                            priority: cols[config.COL_IDS.PRIORITY] || '',
                            dateClosed: cols[config.COL_IDS.DATE_CLOSED] || '',
                            statusValue: colValues[config.COL_IDS.STATUS] || ''
                        };
                        boardItems.push(processed);
                        allItems.push(processed);
                    });
                    cursor = boardData.items_page.cursor;
                    hasMore = !!cursor;
                }

                const startOfToday = new Date();
                startOfToday.setHours(0, 0, 0, 0);
                const getDaysAgo = (d) => {
                    const date = new Date(startOfToday);
                    date.setDate(date.getDate() - d);
                    return date;
                };

                if (boardItems.length > 0) {
                    const openItems = boardItems.filter(i => config.OPEN_STATUSES.includes(i.status));
                    const openCount = openItems.length;
                    currentRunCounts[board.id] = openCount;

                    // Count closed items across 5D, 10D, 30D periods
                    // Use DATE_CLOSED column as authoritative closure date
                    const countClosed = (days) => boardItems.filter(i => {
                        if (!config.CLOSED_STATUSES.includes(i.status)) return false;
                        if (!i.dateClosed) return false;
                        const closedDate = new Date(i.dateClosed);
                        return !isNaN(closedDate.getTime()) && closedDate >= getDaysAgo(days);
                    }).length;

                    const closedLast5 = countClosed(5);
                    const closedLast10 = countClosed(10);
                    const closedLast30 = countClosed(30);

                    // Calculate new items in various periods
                    const newLast5 = boardItems.filter(i => new Date(i.created_at) >= getDaysAgo(5)).length;
                    const newLast10 = boardItems.filter(i => new Date(i.created_at) >= getDaysAgo(10)).length;
                    const newLast30 = boardItems.filter(i => new Date(i.created_at) >= getDaysAgo(30)).length;

                    // Calculate net change in open items
                    let changeVal = 0;
                    let isNewBoard = false;
                    if (comparisonData && comparisonData[board.id] !== undefined) {
                        changeVal = openCount - comparisonData[board.id];
                    } else {
                        // Fallback: flow-based net when no history snapshot exists
                        changeVal = newLast5 - closedLast5;
                    }

                    // Health score: 65% backlog penalty + 35% flow score
                    let healthScore;
                    if (openCount === 0 && newLast5 === 0) {
                        healthScore = 100;
                    } else {
                        const backlogPenalty = 1 / (1 + (openCount / config.OPEN_TARGET));
                        const flowScore = (closedLast5 + newLast5 === 0)
                            ? 1.0
                            : closedLast5 / (closedLast5 + newLast5 + 1);
                        healthScore = Math.round(100 * (0.65 * backlogPenalty + 0.35 * flowScore));
                    }

                    // Determine trend
                    let trend = 'stable';
                    if (changeVal < -2) trend = 'improving';
                    else if (changeVal > 2) trend = 'declining';

                    boardStats.push({
                        id: board.id,
                        name: board.name,
                        totalItems: boardItems.length,
                        newLast5,
                        newLast10,
                        newLast30,
                        openItems: openCount,
                        closedLast5,
                        closedLast10,
                        closedLast30,
                        change: changeVal,
                        isNewBoard,
                        healthScore,
                        trend
                    });

                    // Track aging items
                    openItems.forEach(item => {
                        const ageInDays = Math.floor((now - new Date(item.created_at)) / (24 * 60 * 60 * 1000));
                        if (ageInDays >= config.AGING_THRESHOLDS.SEVERE) {
                            agingItems.severe.push({ ...item, ageInDays });
                        } else if (ageInDays >= config.AGING_THRESHOLDS.CRITICAL) {
                            agingItems.critical.push({ ...item, ageInDays });
                        } else if (ageInDays >= config.AGING_THRESHOLDS.WARNING) {
                            agingItems.warning.push({ ...item, ageInDays });
                        }
                    });
                }
            } catch (err) {
                console.error(`  ❌ Error processing board ${board.name}:`, err.message);
            }
        }));

        // Save history
        historyStore[todayKey] = currentRunCounts;
        const retentionDate = new Date();
        retentionDate.setDate(retentionDate.getDate() - config.HISTORY_RETENTION_DAYS);
        Object.keys(historyStore).forEach(date => {
            if (new Date(date) < retentionDate) delete historyStore[date];
        });
        fs.writeFileSync(HISTORY_FILE, JSON.stringify(historyStore, null, 2), 'utf8');

        // Get recent items for detail section
        const tenDaysAgo = new Date();
        tenDaysAgo.setDate(tenDaysAgo.getDate() - 10);
        const recentItems = allItems.filter(i => new Date(i.created_at) >= tenDaysAgo);

        // Generate outputs
        const htmlContent = generateEmailHTML(workspaceName, boardStats, recentItems, comparisonDateUsed, agingItems, historyStore);
        const csvContent = generateCSV(boardStats, recentItems, agingItems);

        const onedrivePath = process.env.NODE_ENV !== 'production' ? path.join(os.homedir(), 'OneDrive - Power Solutions Int\'l', 'MondayReports') : null;
        if (onedrivePath && !fs.existsSync(onedrivePath)) fs.mkdirSync(onedrivePath, { recursive: true });

        // CSV goes to local exports folder (not OneDrive to avoid triggering Power Automate)
        const exportsPath = path.join(__dirname, 'exports');
        if (!fs.existsSync(exportsPath)) fs.mkdirSync(exportsPath, { recursive: true });

        const dateStr = new Date().toISOString().split('T')[0];
        const timeStr = new Date().getHours().toString().padStart(2, '0') + new Date().getMinutes().toString().padStart(2, '0');
        const htmlFileName = `email-report-${dateStr}-${timeStr}.html`;
        const csvFileName = `report-data-${dateStr}-${timeStr}.csv`;

        if (onedrivePath) {
          fs.writeFileSync(path.join(onedrivePath, htmlFileName), htmlContent, 'utf8');
        }
        fs.writeFileSync(path.join(exportsPath, csvFileName), csvContent, 'utf8');

        // Mark as run today
        fs.writeFileSync(LAST_RUN_FILE, todayKey, 'utf8');

        console.log(`\n✅ Success! Reports saved:`);
        if (onedrivePath) {
          console.log(`   HTML: ${onedrivePath}\\${htmlFileName}`);
        } else {
          console.log('   HTML: skipped (production mode)');
        }
        console.log(`   CSV:  ${exportsPath}\\${csvFileName}`);

        // Send email via MS Graph (preferred) or SMTP fallback
        await sendEmailIfConfigured(htmlContent, csvContent, csvFileName);

    } catch (err) {
        console.error('❌ Fatal Error:', err.message);
    }
}

function generateCSV(boardStats, recentItems, agingItems) {
    const lines = [];

    // Board Summary Section
    lines.push('BOARD SUMMARY');
    lines.push('Board Name,New (5D),New (10D),New (30D),Open Items,Closed (5D),Closed (10D),Closed (30D),Net (5D),Health Score,Trend,Total Items');
    boardStats.sort((a, b) => b.newLast10 - a.newLast10).forEach(b => {
        lines.push(`"${b.name}",${b.newLast5},${b.newLast10},${b.newLast30},${b.openItems},${b.closedLast5},${b.closedLast10},${b.closedLast30},${b.change},${b.healthScore}%,${b.trend},${b.totalItems}`);
    });

    lines.push('');
    lines.push('RECENT ITEMS (Last 10 Days)');
    lines.push('Date,Board,CX Alloy,Item Name,Group,Unit,ROMP,Status,Priority,Assigned');
    recentItems.sort((a, b) => new Date(b.created_at) - new Date(a.created_at)).forEach(item => {
        const d = new Date(item.created_at);
        const dateStr = `${d.getMonth()+1}/${d.getDate()}/${d.getFullYear()}`;
        lines.push(`${dateStr},"${item.board}","${item.cxAlloy}","${item.name.replace(/"/g, '""')}","${item.group}","${item.unit}","${item.romp}","${item.status}","${item.priority}","${item.person}"`);
    });

    lines.push('');
    lines.push('AGING ITEMS');
    lines.push('Age Category,Days Old,Board,CX Alloy,Item Name,Status,Assigned');

    agingItems.severe.sort((a, b) => b.ageInDays - a.ageInDays).forEach(item => {
        lines.push(`90+ Days,${item.ageInDays},"${item.board}","${item.cxAlloy}","${item.name.replace(/"/g, '""')}","${item.status}","${item.person}"`);
    });
    agingItems.critical.sort((a, b) => b.ageInDays - a.ageInDays).forEach(item => {
        lines.push(`60-89 Days,${item.ageInDays},"${item.board}","${item.cxAlloy}","${item.name.replace(/"/g, '""')}","${item.status}","${item.person}"`);
    });
    agingItems.warning.sort((a, b) => b.ageInDays - a.ageInDays).forEach(item => {
        lines.push(`30-59 Days,${item.ageInDays},"${item.board}","${item.cxAlloy}","${item.name.replace(/"/g, '""')}","${item.status}","${item.person}"`);
    });

    return lines.join('\n');
}

// ── Chart Helper Functions (HTML/CSS only — no SVG for max compatibility) ────

function prepareChartData(historyStore, boardStats) {
    const chartData = {};
    const sortedDates = Object.keys(historyStore).sort();

    for (const board of boardStats) {
        const points = [];
        for (const date of sortedDates) {
            if (historyStore[date] && historyStore[date][board.id] !== undefined) {
                points.push({ date, count: historyStore[date][board.id] });
            }
        }
        chartData[board.id] = points;
    }
    return chartData;
}

function generateSparklineHTML(points, days) {
    if (!points || points.length < 3) return '<span style="color: #94a3b8; font-size: 11px;">&#8212;</span>';

    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const filtered = points.filter(p => new Date(p.date) >= cutoff);
    if (filtered.length < 3) return '<span style="color: #94a3b8; font-size: 11px;">&#8212;</span>';

    // Sample down to ~15 bars max for compact display
    let sampled = filtered;
    if (filtered.length > 15) {
        sampled = [];
        for (let i = 0; i < 15; i++) {
            const idx = Math.round((i / 14) * (filtered.length - 1));
            sampled.push(filtered[idx]);
        }
    }

    const counts = sampled.map(p => p.count);
    const maxVal = Math.max(...counts) || 1;
    const maxH = 24; // max bar height in pixels

    // Determine color by net direction
    const first = sampled[0].count;
    const last = sampled[sampled.length - 1].count;
    const barColor = last < first ? '#10b981' : (last > first ? '#ef4444' : '#64748b');

    // Email-safe bar chart with start/end value labels flanking the bars
    // Layout: [startVal] [bars] [endVal]
    let barsHtml = `<table cellpadding="0" cellspacing="1" border="0"><tr>`;
    for (const pt of sampled) {
        const barH = Math.max(Math.round((pt.count / maxVal) * maxH), 2);
        const spacerH = maxH - barH;
        barsHtml += `<td width="5" valign="bottom" style="padding: 0;">` +
            `<table cellpadding="0" cellspacing="0" border="0" width="5">` +
            (spacerH > 0 ? `<tr><td height="${spacerH}" style="font-size:1px; line-height:1px;">&nbsp;</td></tr>` : '') +
            `<tr><td height="${barH}" bgcolor="${barColor}" style="font-size:1px; line-height:1px;">&nbsp;</td></tr>` +
            `</table></td>`;
    }
    barsHtml += '</tr></table>';

    const html = `<table cellpadding="0" cellspacing="0" border="0" style="margin: 0 auto;"><tr>` +
        `<td valign="bottom" style="padding: 0 3px 1px 0; font-size: 9px; font-weight: bold; color: #94a3b8;">${first}</td>` +
        `<td valign="bottom" style="padding: 0;">${barsHtml}</td>` +
        `<td valign="bottom" style="padding: 0 0 1px 3px; font-size: 9px; font-weight: bold; color: ${barColor};">${last}</td>` +
        `</tr></table>`;
    return html;
}

function generateDetailChartHTML(chartData, boardStats) {
    // Pick top 8 boards by current open items (with at least 3 data points)
    const eligible = boardStats
        .filter(b => chartData[b.id] && chartData[b.id].length >= 3)
        .sort((a, b) => b.openItems - a.openItems)
        .slice(0, 8);

    if (eligible.length === 0) return '';

    const palette = ['#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899', '#06b6d4', '#f97316'];

    // Build per-board trend rows
    let rows = '';
    const maxH = 36; // max bar height in pixels for detail chart

    eligible.forEach((board, bIdx) => {
        const color = palette[bIdx % palette.length];
        const points = chartData[board.id];
        const counts = points.map(p => p.count);
        const maxVal = Math.max(...counts) || 1;
        const minVal = Math.min(...counts);
        const first = points[0].count;
        const last = points[points.length - 1].count;
        const change = last - first;
        const changeColor = change < 0 ? '#10b981' : (change > 0 ? '#ef4444' : '#64748b');
        const changeText = change > 0 ? `+${change}` : `${change}`;

        // Sample to ~30 bars for the wider chart
        let sampled = points;
        if (points.length > 30) {
            sampled = [];
            for (let i = 0; i < 30; i++) {
                const idx = Math.round((i / 29) * (points.length - 1));
                sampled.push(points[idx]);
            }
        }

        // Date range label
        const startDate = new Date(points[0].date);
        const endDate = new Date(points[points.length - 1].date);
        const startLabel = `${startDate.getMonth() + 1}/${startDate.getDate()}`;
        const endLabel = `${endDate.getMonth() + 1}/${endDate.getDate()}`;

        // Email-safe bar chart: stacked 2-cell columns (spacer + colored bar) with pixel heights
        let bars = `<table cellpadding="0" cellspacing="1" border="0" width="100%"><tr>`;
        for (const pt of sampled) {
            const barH = Math.max(Math.round((pt.count / maxVal) * maxH), 2);
            const spacerH = maxH - barH;
            bars += `<td valign="bottom" style="padding: 0;">` +
                `<table cellpadding="0" cellspacing="0" border="0" width="100%">` +
                (spacerH > 0 ? `<tr><td height="${spacerH}" style="font-size:1px; line-height:1px;">&nbsp;</td></tr>` : '') +
                `<tr><td height="${barH}" bgcolor="${color}" style="font-size:1px; line-height:1px;">&nbsp;</td></tr>` +
                `</table></td>`;
        }
        bars += '</tr></table>';

        const rowBg = bIdx % 2 === 1 ? ' bgcolor="#f8fafc"' : '';

        rows += `
        <tr${rowBg} style="border-bottom: 1px solid #e2e8f0;">
            <td style="padding: 8px; font-size: 11px; font-weight: bold; color: #334155; vertical-align: middle; min-width: 80px; word-break: break-word;">
                <table cellpadding="0" cellspacing="0" border="0"><tr>
                    <td width="10" height="10" bgcolor="${color}" style="font-size:1px;">&nbsp;</td>
                    <td style="padding-left: 6px; font-size: 11px; font-weight: bold; color: #334155;">${board.name}</td>
                </tr></table>
            </td>
            <td align="center" style="padding: 8px 6px; vertical-align: middle; white-space: nowrap;">
                <span style="font-size: 16px; font-weight: bold; color: ${changeColor};">${last}</span>
                <br><span style="font-size: 9px; color: #94a3b8;">NOW</span>
            </td>
            <td style="padding: 8px; vertical-align: middle;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 2px;">
                    <tr>
                        <td style="font-size: 9px; color: #94a3b8;">Peak: <strong style="color: #64748b;">${maxVal}</strong></td>
                        <td align="right" style="font-size: 9px; color: #94a3b8;">Low: <strong style="color: #64748b;">${minVal}</strong></td>
                    </tr>
                </table>
                ${bars}
                <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top: 2px;">
                    <tr>
                        <td style="font-size: 9px; color: #94a3b8;">${startLabel} (${first} open)</td>
                        <td align="right" style="font-size: 9px; color: #94a3b8;">${endLabel} (${last} open)</td>
                    </tr>
                </table>
            </td>
            <td align="center" style="padding: 8px 6px; vertical-align: middle; white-space: nowrap;">
                <span style="font-size: 14px; font-weight: bold; color: ${changeColor};">${changeText}</span>
                <br><span style="font-size: 9px; color: #94a3b8;">NET</span>
            </td>
        </tr>`;
    });

    return rows;
}

function generateEmailHTML(workspaceName, boardStats, recentItems, comparisonDate, agingItems, historyStore) {
    const today = new Date().toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' });

    // Prepare chart data from history
    const chartData = prepareChartData(historyStore || {}, boardStats);
    const detailChartRows = generateDetailChartHTML(chartData, boardStats);

    // Calculate totals
    const totalNew5 = boardStats.reduce((sum, b) => sum + b.newLast5, 0);
    const totalNew10 = boardStats.reduce((sum, b) => sum + b.newLast10, 0);
    const totalNew30 = boardStats.reduce((sum, b) => sum + b.newLast30, 0);
    const totalOpen = boardStats.reduce((sum, b) => sum + b.openItems, 0);
    const totalClosed5 = boardStats.reduce((sum, b) => sum + b.closedLast5, 0);
    const totalClosed10 = boardStats.reduce((sum, b) => sum + b.closedLast10, 0);
    const totalClosed30 = boardStats.reduce((sum, b) => sum + b.closedLast30, 0);
    const totalChange = boardStats.reduce((sum, b) => sum + b.change, 0);
    // Weighted average health: weight by volume
    const totalWeight = boardStats.reduce((sum, b) => sum + b.openItems + b.newLast5 + b.closedLast5, 0);
    const avgHealthScore = totalWeight > 0
        ? Math.round(boardStats.reduce((sum, b) => {
            const weight = b.openItems + b.newLast5 + b.closedLast5;
            return sum + (b.healthScore * weight);
        }, 0) / totalWeight)
        : 100;

    // Aging totals
    const totalAging = agingItems.warning.length + agingItems.critical.length + agingItems.severe.length;

    const maxNew5 = Math.max(...boardStats.map(b => b.newLast5), 1);
    const maxNew10 = Math.max(...boardStats.map(b => b.newLast10), 1);
    const maxOpen = Math.max(...boardStats.map(b => b.openItems), 1);

    const getHeatColor = (v, m) => v === 0 ? '#ffffff' : (v/m >= 0.7 ? '#fee2e2' : (v/m >= 0.4 ? '#fef3c7' : '#d1fae5'));
    const getChangeColor = (val) => val > 0 ? '#dc2626' : (val < 0 ? '#10b981' : '#999999');
    const getHealthColor = (score) => score >= 70 ? '#10b981' : (score >= 40 ? '#f59e0b' : '#dc2626');
    const getTrendArrow = (trend) => {
        if (trend === 'improving') return '<span style="color: #10b981; font-size: 24px; font-weight: bold;">&#9660;</span>'; // Down triangle (good)
        if (trend === 'declining') return '<span style="color: #f43f5e; font-size: 24px; font-weight: bold;">&#9650;</span>'; // Up triangle (bad)
        return '<span style="color: #64748b; font-size: 20px; font-weight: bold;">&#9654;</span>'; // Right triangle (stable)
    };

    const itemsByBoard = {};
    recentItems.forEach(item => {
        if (!itemsByBoard[item.board]) itemsByBoard[item.board] = [];
        itemsByBoard[item.board].push(item);
    });

    const sortedBoardNames = Object.keys(itemsByBoard).sort((a, b) => a.localeCompare(b));

    // Priority color helper
    const getPriorityBadge = (priority) => {
        if (!priority) return '';
        const colors = {
            'Critical': { bg: '#dc2626', text: '#ffffff' },
            'High': { bg: '#f97316', text: '#ffffff' },
            'Medium': { bg: '#eab308', text: '#000000' },
            'Low': { bg: '#6b7280', text: '#ffffff' }
        };
        const style = colors[priority] || { bg: '#9ca3af', text: '#000000' };
        return `<span style="background-color: ${style.bg}; color: ${style.text}; padding: 2px 6px; font-size: 9px; font-weight: bold; border-radius: 2px; margin-left: 8px;">${priority.toUpperCase()}</span>`;
    };

    return `<!DOCTYPE html>
<html>
<head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; font-family: Segoe UI, Arial, sans-serif; background-color: #f5f5f5;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #f5f5f5; padding: 15px;">
        <tr>
            <td align="center">
                <table width="95%" cellpadding="0" cellspacing="0" border="0" style="max-width: 1400px; background-color: #ffffff; border: 1px solid #dddddd; width: 95% !important;">

                    <tr>
                        <td style="background: linear-gradient(135deg, #10b981 0%, #14b8a6 100%); background-color: #10b981; padding: 28px 35px; text-align: center; color: #ffffff; border-radius: 16px 16px 0 0;">
                            <h1 style="margin: 0; font-size: 28px; font-weight: bold; letter-spacing: -0.5px;">Enclosure Support Weekly Report</h1>
                            <p style="margin: 6px 0 0 0; font-size: 13px; opacity: 0.9;">
                                <span style="font-size: 14px; margin-right: 4px;">&#128197;</span>
                                ${workspaceName} &bull; ${today}
                            </p>
                        </td>
                    </tr>

                    <!-- Velocity Metrics Section -->
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
                                                    <div style="font-size: 32px; font-weight: bold; color: #10b981;">${totalNew5}</div>
                                                    <div style="position: absolute; bottom: 3px; right: 8px; font-size: 44px; color: #6ee7b7;">&#10010;</div>
                                                </td>
                                                <td width="33.33%" style="background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 15px 18px; position: relative;">
                                                    <div style="font-size: 10px; font-weight: bold; color: #64748b; text-transform: uppercase; margin-bottom: 3px;">New (10 Days)</div>
                                                    <div style="font-size: 32px; font-weight: bold; color: #10b981;">${totalNew10}</div>
                                                    <div style="position: absolute; bottom: 3px; right: 8px; font-size: 44px; color: #6ee7b7;">&#8635;</div>
                                                </td>
                                                <td width="33.33%" style="background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 15px 18px; position: relative;">
                                                    <div style="font-size: 10px; font-weight: bold; color: #64748b; text-transform: uppercase; margin-bottom: 3px;">New (30 Days)</div>
                                                    <div style="font-size: 32px; font-weight: bold; color: #10b981;">${totalNew30}</div>
                                                    <div style="position: absolute; bottom: 3px; right: 8px; font-size: 44px; color: #6ee7b7;">&#128197;</div>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                    <!-- Status & Health Section -->
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
                                                <td width="16.66%" style="background: #ffffff; border: 1px solid #e2e8f0; border-left: 4px solid #f43f5e; border-radius: 8px; padding: 12px; position: relative;">
                                                    <div style="font-size: 9px; font-weight: bold; color: #94a3b8; text-transform: uppercase;">Total Open</div>
                                                    <div style="font-size: 24px; font-weight: bold; color: #e11d48;">${totalOpen}</div>
                                                    <div style="position: absolute; top: 5px; right: 8px; font-size: 32px; color: #fda4af;">&#128194;</div>
                                                </td>
                                                <td width="16.66%" style="background: #ffffff; border: 1px solid #e2e8f0; border-left: 4px solid #3b82f6; border-radius: 8px; padding: 12px; position: relative;">
                                                    <div style="font-size: 9px; font-weight: bold; color: #94a3b8; text-transform: uppercase;">Closed (5D)</div>
                                                    <div style="font-size: 24px; font-weight: bold; color: #2563eb;">${totalClosed5}</div>
                                                    <div style="position: absolute; top: 5px; right: 8px; font-size: 32px; color: #93c5fd;">&#9989;</div>
                                                </td>
                                                <td width="16.66%" style="background: #ffffff; border: 1px solid #e2e8f0; border-left: 4px solid #6366f1; border-radius: 8px; padding: 12px; position: relative;">
                                                    <div style="font-size: 9px; font-weight: bold; color: #94a3b8; text-transform: uppercase;">Closed (10D)</div>
                                                    <div style="font-size: 24px; font-weight: bold; color: #4f46e5;">${totalClosed10}</div>
                                                    <div style="position: absolute; top: 5px; right: 8px; font-size: 32px; color: #a5b4fc;">&#9989;</div>
                                                </td>
                                                <td width="16.66%" style="background: #ffffff; border: 1px solid #e2e8f0; border-left: 4px solid #8b5cf6; border-radius: 8px; padding: 12px; position: relative;">
                                                    <div style="font-size: 9px; font-weight: bold; color: #94a3b8; text-transform: uppercase;">Closed (30D)</div>
                                                    <div style="font-size: 24px; font-weight: bold; color: #7c3aed;">${totalClosed30}</div>
                                                    <div style="position: absolute; top: 5px; right: 8px; font-size: 32px; color: #c4b5fd;">&#9989;</div>
                                                </td>
                                                <td width="16.66%" style="background: #ffffff; border: 1px solid #e2e8f0; border-left: 4px solid #f97316; border-radius: 8px; padding: 12px; position: relative;">
                                                    <div style="font-size: 9px; font-weight: bold; color: #94a3b8; text-transform: uppercase;">Aging (30+D)</div>
                                                    <div style="font-size: 24px; font-weight: bold; color: #ea580c;">${totalAging}</div>
                                                    <div style="position: absolute; top: 5px; right: 8px; font-size: 32px; color: #fdba74;">&#9203;</div>
                                                </td>
                                                <td width="16.66%" style="background: #ffffff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 12px;">
                                                    <div style="font-size: 9px; font-weight: bold; color: #94a3b8; text-transform: uppercase;">Avg Health</div>
                                                    <table cellpadding="0" cellspacing="0" border="0" width="100%">
                                                        <tr>
                                                            <td><div style="font-size: 20px; font-weight: bold; color: ${getHealthColor(avgHealthScore)};">${avgHealthScore}%</div></td>
                                                            <td align="right">
                                                                <div style="width: 36px; height: 36px; border-radius: 50%; border: 4px solid #e2e8f0; border-top-color: ${getHealthColor(avgHealthScore)}; border-right-color: ${avgHealthScore >= 50 ? getHealthColor(avgHealthScore) : '#e2e8f0'}; border-bottom-color: ${avgHealthScore >= 75 ? getHealthColor(avgHealthScore) : '#e2e8f0'}; display: inline-block;"></div>
                                                            </td>
                                                        </tr>
                                                    </table>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                    <!-- Board Activity Table -->
                    <tr>
                        <td style="padding: 20px 25px; background-color: #ffffff;">
                            <table cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 12px;">
                                <tr>
                                    <td style="font-size: 22px; color: #10b981; vertical-align: middle;">&#9776;</td>
                                    <td style="font-size: 15px; font-weight: bold; color: #1e293b; text-transform: uppercase; letter-spacing: 1.5px; padding-left: 10px; vertical-align: middle;">Board Activity Summary</td>
                                </tr>
                            </table>
                            <table width="100%" cellpadding="0" cellspacing="0" style="border: 1px solid #e2e8f0; border-collapse: collapse; border-radius: 8px; overflow: hidden;">
                                <tr style="background-color: #0f172a; color: #ffffff; font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;">
                                    <th align="left" style="padding: 10px 8px; min-width: 80px;">Board Name</th>
                                    <th style="padding: 10px 4px; text-align: center; white-space: nowrap;">5D</th>
                                    <th style="padding: 10px 4px; text-align: center; white-space: nowrap;">10D</th>
                                    <th style="padding: 10px 4px; text-align: center; white-space: nowrap;">30D</th>
                                    <th style="padding: 10px 4px; text-align: center; white-space: nowrap;">Open</th>
                                    <th style="padding: 10px 4px; text-align: center; white-space: nowrap;">Cl 5D</th>
                                    <th style="padding: 10px 4px; text-align: center; white-space: nowrap;">Net 5D</th>
                                    <th style="padding: 10px 2px; text-align: center;">&#8597;</th>
                                    <th style="padding: 10px 4px; text-align: center; min-width: 100px;">Open (30D)</th>
                                    <th style="padding: 10px 4px; text-align: center; white-space: nowrap;">Hlth</th>
                                    <th style="padding: 10px 4px; text-align: center; white-space: nowrap;">Tot</th>
                                </tr>
                                ${boardStats.sort((a,b) => b.newLast10 - a.newLast10).map((b, idx) => `
                                <tr style="font-size: 12px; background-color: ${idx % 2 === 0 ? '#ffffff' : '#f8fafc'}; border-bottom: 1px solid #e2e8f0;">
                                    <td style="font-weight: bold; padding: 8px; color: #334155; word-break: break-word;">${b.name}${b.isNewBoard ? ' <span style="background: #8b5cf6; color: white; padding: 1px 5px; font-size: 9px; border-radius: 3px;">NEW</span>' : ''}</td>
                                    <td align="center" style="padding: 8px 4px; background-color: ${getHeatColor(b.newLast5, maxNew5)}; white-space: nowrap;">${b.newLast5}</td>
                                    <td align="center" style="padding: 8px 4px; background-color: ${getHeatColor(b.newLast10, maxNew10)}; white-space: nowrap;">${b.newLast10}</td>
                                    <td align="center" style="padding: 8px 4px; white-space: nowrap;">${b.newLast30}</td>
                                    <td align="center" style="padding: 8px 4px; background-color: ${getHeatColor(b.openItems, maxOpen)}; font-weight: bold; white-space: nowrap;">${b.openItems}</td>
                                    <td align="center" style="padding: 8px 4px; color: #2563eb; font-weight: bold; white-space: nowrap;">${b.closedLast5}</td>
                                    <td align="center" style="padding: 8px 4px; color: ${getChangeColor(b.change)}; font-weight: bold; white-space: nowrap;">${b.isNewBoard ? '<span style="color: #8b5cf6;">NEW</span>' : (b.change > 0 ? '+' : '') + b.change}</td>
                                    <td align="center" style="padding: 8px 2px;">${getTrendArrow(b.trend)}</td>
                                    <td align="center" style="padding: 4px 2px;">${generateSparklineHTML(chartData[b.id], 30)}</td>
                                    <td align="center" style="padding: 8px 4px; color: ${getHealthColor(b.healthScore)}; font-weight: bold; white-space: nowrap;">${b.healthScore}%</td>
                                    <td align="center" style="padding: 8px 4px; color: #94a3b8; white-space: nowrap;">${b.totalItems}</td>
                                </tr>`).join('')}
                            </table>
                            <table width="100%" cellpadding="8" cellspacing="0" border="0" style="background-color: #f8fafc; margin-top: 10px; border-radius: 6px;">
                                <tr>
                                    <td style="font-size: 10px; color: #64748b; text-align: center;">
                                        <strong style="color: #334155;">Cl 5D / Net 5D:</strong> Closed &amp; net change over 5 days
                                        &nbsp;|&nbsp;
                                        <strong style="color: #334155;">Trend:</strong>
                                        <span style="color: #10b981; font-weight: bold;">&#9660; Improving</span>
                                        <span style="color: #64748b; font-weight: bold;">&#9654; Stable</span>
                                        <span style="color: #f43f5e; font-weight: bold;">&#9650; Declining</span>
                                        &nbsp;|&nbsp;
                                        <strong style="color: #334155;">Open (30D):</strong> Daily open item count &mdash;
                                        <span style="color: #10b981; font-weight: bold;">Green</span> = decreasing
                                        <span style="color: #ef4444; font-weight: bold;">Red</span> = increasing
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                    <!-- Open Item Trends Chart -->
                    ${detailChartRows ? `
                    <tr>
                        <td style="padding: 20px 25px; background-color: #ffffff; border-top: 1px solid #e2e8f0;">
                            <table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom: 12px;">
                                <tr>
                                    <td style="font-size: 24px; color: #3b82f6; vertical-align: middle;">&#128200;</td>
                                    <td style="font-size: 15px; font-weight: bold; color: #1e293b; text-transform: uppercase; letter-spacing: 1.5px; padding-left: 10px; vertical-align: middle;">Open Item Trends</td>
                                    <td style="width: 100%; padding-left: 15px; vertical-align: middle;"><div style="height: 2px; background: linear-gradient(90deg, #e2e8f0 0%, transparent 100%); background-color: #e2e8f0;"></div></td>
                                </tr>
                            </table>
                            <p style="font-size: 11px; color: #64748b; margin: 0 0 12px 0;">Each bar = one day's snapshot of open items for that board. Taller bars mean more open issues on that day. Top 8 boards by current open count shown.</p>
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border: 1px solid #e2e8f0; border-collapse: collapse; border-radius: 8px; overflow: hidden;">
                                <tr style="background-color: #0f172a; color: #ffffff; font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;">
                                    <th align="left" style="padding: 10px 8px; min-width: 80px;">Board</th>
                                    <th style="padding: 10px 6px; text-align: center; white-space: nowrap;">Now</th>
                                    <th style="padding: 10px 8px; text-align: center; min-width: 120px;">Daily Open Item Count</th>
                                    <th style="padding: 10px 6px; text-align: center; white-space: nowrap;">Net</th>
                                </tr>
                                ${detailChartRows}
                            </table>
                        </td>
                    </tr>` : ''}

                    <!-- Aging Items Section -->
                    ${totalAging > 0 ? `
                    <tr>
                        <td style="padding: 25px; background-color: #2d3748;">
                            <h2 style="font-size: 20px; margin: 0 0 20px 0; color: #ffffff;">Aging Items Requiring Attention</h2>

                            ${agingItems.severe.length > 0 ? `
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 20px;">
                                <tr>
                                    <td style="background-color: #dc2626; padding: 12px 15px;">
                                        <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                            <tr>
                                                <td style="color: #ffffff; font-weight: bold; font-size: 14px;">CRITICAL: 90+ Days</td>
                                                <td align="right" style="color: #ffffff; font-weight: bold; font-size: 14px;">${agingItems.severe.length} items</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="background-color: #ffffff; border: 2px solid #dc2626; border-top: none;">
                                        <table width="100%" cellpadding="10" cellspacing="0" border="0" style="border-collapse: collapse;">
                                            <tr style="background-color: #fee2e2;">
                                                <th align="center" width="60" style="font-size: 11px; color: #991b1b; border-bottom: 1px solid #fca5a5;">DAYS</th>
                                                <th align="left" width="20%" style="font-size: 11px; color: #991b1b; border-bottom: 1px solid #fca5a5;">BOARD</th>
                                                <th align="left" style="font-size: 11px; color: #991b1b; border-bottom: 1px solid #fca5a5;">ITEM</th>
                                                <th align="left" width="15%" style="font-size: 11px; color: #991b1b; border-bottom: 1px solid #fca5a5;">ASSIGNED</th>
                                            </tr>
                                            ${agingItems.severe.slice(0, 10).map((item, idx) => `
                                            <tr style="font-size: 12px; background-color: ${idx % 2 === 0 ? '#ffffff' : '#fef2f2'};">
                                                <td align="center" style="font-weight: bold; color: #dc2626; border-bottom: 1px solid #fee2e2;">${item.ageInDays}</td>
                                                <td style="color: #475569; border-bottom: 1px solid #fee2e2;">${item.board}</td>
                                                <td style="border-bottom: 1px solid #fee2e2;"><a href="https://${config.MONDAY_SLUG}.monday.com/boards/${item.boardId}/pulses/${item.id}" style="color: #111111; text-decoration: none;">${item.cxAlloy ? `<strong style="color: #dc2626;">[${item.cxAlloy}]</strong> ` : ''}${item.name}</a></td>
                                                <td style="color: #475569; border-bottom: 1px solid #fee2e2;">${item.person}</td>
                                            </tr>`).join('')}
                                        </table>
                                    </td>
                                </tr>
                            </table>` : ''}

                            ${agingItems.critical.length > 0 ? `
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 20px;">
                                <tr>
                                    <td style="background-color: #ea580c; padding: 12px 15px;">
                                        <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                            <tr>
                                                <td style="color: #ffffff; font-weight: bold; font-size: 14px;">OVERDUE: 60-89 Days</td>
                                                <td align="right" style="color: #ffffff; font-weight: bold; font-size: 14px;">${agingItems.critical.length} items</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="background-color: #ffffff; border: 2px solid #ea580c; border-top: none;">
                                        <table width="100%" cellpadding="10" cellspacing="0" border="0" style="border-collapse: collapse;">
                                            <tr style="background-color: #ffedd5;">
                                                <th align="center" width="60" style="font-size: 11px; color: #9a3412; border-bottom: 1px solid #fdba74;">DAYS</th>
                                                <th align="left" width="20%" style="font-size: 11px; color: #9a3412; border-bottom: 1px solid #fdba74;">BOARD</th>
                                                <th align="left" style="font-size: 11px; color: #9a3412; border-bottom: 1px solid #fdba74;">ITEM</th>
                                                <th align="left" width="15%" style="font-size: 11px; color: #9a3412; border-bottom: 1px solid #fdba74;">ASSIGNED</th>
                                            </tr>
                                            ${agingItems.critical.slice(0, 10).map((item, idx) => `
                                            <tr style="font-size: 12px; background-color: ${idx % 2 === 0 ? '#ffffff' : '#fff7ed'};">
                                                <td align="center" style="font-weight: bold; color: #ea580c; border-bottom: 1px solid #ffedd5;">${item.ageInDays}</td>
                                                <td style="color: #475569; border-bottom: 1px solid #ffedd5;">${item.board}</td>
                                                <td style="border-bottom: 1px solid #ffedd5;"><a href="https://${config.MONDAY_SLUG}.monday.com/boards/${item.boardId}/pulses/${item.id}" style="color: #111111; text-decoration: none;">${item.cxAlloy ? `<strong style="color: #ea580c;">[${item.cxAlloy}]</strong> ` : ''}${item.name}</a></td>
                                                <td style="color: #475569; border-bottom: 1px solid #ffedd5;">${item.person}</td>
                                            </tr>`).join('')}
                                        </table>
                                    </td>
                                </tr>
                            </table>` : ''}

                            ${agingItems.warning.length > 0 ? `
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 20px;">
                                <tr>
                                    <td style="background-color: #3b82f6; padding: 12px 15px;">
                                        <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                            <tr>
                                                <td style="color: #ffffff; font-weight: bold; font-size: 14px;">NEEDS ATTENTION: 30-59 Days</td>
                                                <td align="right" style="color: #ffffff; font-weight: bold; font-size: 14px;">${agingItems.warning.length} items</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="background-color: #ffffff; border: 2px solid #3b82f6; border-top: none;">
                                        <table width="100%" cellpadding="10" cellspacing="0" border="0" style="border-collapse: collapse;">
                                            <tr style="background-color: #dbeafe;">
                                                <th align="center" width="60" style="font-size: 11px; color: #1e40af; border-bottom: 1px solid #93c5fd;">DAYS</th>
                                                <th align="left" width="20%" style="font-size: 11px; color: #1e40af; border-bottom: 1px solid #93c5fd;">BOARD</th>
                                                <th align="left" style="font-size: 11px; color: #1e40af; border-bottom: 1px solid #93c5fd;">ITEM</th>
                                                <th align="left" width="15%" style="font-size: 11px; color: #1e40af; border-bottom: 1px solid #93c5fd;">ASSIGNED</th>
                                            </tr>
                                            ${agingItems.warning.slice(0, 10).map((item, idx) => `
                                            <tr style="font-size: 12px; background-color: ${idx % 2 === 0 ? '#ffffff' : '#eff6ff'};">
                                                <td align="center" style="font-weight: bold; color: #3b82f6; border-bottom: 1px solid #dbeafe;">${item.ageInDays}</td>
                                                <td style="color: #475569; border-bottom: 1px solid #dbeafe;">${item.board}</td>
                                                <td style="border-bottom: 1px solid #dbeafe;"><a href="https://${config.MONDAY_SLUG}.monday.com/boards/${item.boardId}/pulses/${item.id}" style="color: #111111; text-decoration: none;">${item.cxAlloy ? `<strong style="color: #3b82f6;">[${item.cxAlloy}]</strong> ` : ''}${item.name}</a></td>
                                                <td style="color: #475569; border-bottom: 1px solid #dbeafe;">${item.person}</td>
                                            </tr>`).join('')}
                                        </table>
                                    </td>
                                </tr>
                            </table>` : ''}

                            <p style="font-size: 11px; color: #a0aec0; margin: 10px 0 0 0;">Showing up to 10 items per category. See CSV export for complete list.</p>
                        </td>
                    </tr>` : ''}

                    <!-- Recent Items Detail -->
                    <tr>
                        <td style="padding: 25px; background-color: #f9f9f9;">
                            <h2 style="font-size: 20px; margin-bottom: 20px; color: #333333;">Recent Requests Detail (Last 10 Days)</h2>
                            ${sortedBoardNames.map(name => {
                                const stats = boardStats.find(s => s.name === name) || { openItems: 0 };
                                const sortedItems = itemsByBoard[name].sort((a,b) => new Date(b.created_at) - new Date(a.created_at));

                                return `
                                <table width="100%" cellpadding="0" cellspacing="0" style="background: #ffffff; border: 1px solid #dddddd; margin-bottom: 30px; border-collapse: collapse;">
                                    <tr>
                                        <td style="background-color: #2d3748; color: #ffffff; padding: 15px 20px;">
                                            <table width="100%" cellpadding="0" cellspacing="0">
                                                <tr>
                                                    <td style="font-weight: bold; font-size: 16px; color: #ffffff;">${name}</td>
                                                    <td align="right">
                                                        <span style="background-color: #4a5568; color: #ffffff; padding: 4px 10px; font-size: 11px; font-weight: bold; border-radius: 3px; margin-right: 8px;">NEW: ${sortedItems.length}</span>
                                                        <span style="background-color: #ffffff; color: #dc2626; padding: 4px 10px; font-size: 11px; font-weight: bold; border-radius: 3px; border: 1px solid #dc2626;">OPEN: ${stats.openItems}</span>
                                                    </td>
                                                </tr>
                                            </table>
                                        </td>
                                    </tr>
                                    ${sortedItems.map((item, idx) => {
                                        const d = new Date(item.created_at);
                                        const dStr = (d.getMonth()+1).toString().padStart(2,'0') + '/' + d.getDate().toString().padStart(2,'0');
                                        const itemUrl = "https://" + config.MONDAY_SLUG + ".monday.com/boards/" + item.boardId + "/pulses/" + item.id;

                                        return `
                                        <tr style="${idx < sortedItems.length - 1 ? 'border-bottom: 1px solid #eeeeee;' : ''}">
                                            <td style="padding: 20px;">
                                                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                                    <tr>
                                                        <td width="60" style="font-size: 12px; color: #333333; vertical-align: top; font-weight: bold; padding-top: 5px;">${dStr}</td>
                                                        <td style="vertical-align: top;">
                                                            <div style="margin-bottom: 12px;">
                                                                ${item.cxAlloy ? `
                                                                <table align="left" cellpadding="0" cellspacing="0" style="margin-right: 15px; margin-bottom: 5px;">
                                                                    <tr><td style="border: 2px solid #333333; padding: 2px 8px;">
                                                                        <a href="${itemUrl}" target="_blank" style="color: #111111; font-size: 11px; font-weight: 900; text-decoration: none;">${item.cxAlloy}</a>
                                                                    </td></tr>
                                                                </table>` : ''}
                                                                <a href="${itemUrl}" target="_blank" style="font-weight: bold; color: #111111; font-size: 16px; text-decoration: none;">${item.name}</a>${getPriorityBadge(item.priority)}
                                                            </div>
                                                            <table cellpadding="0" cellspacing="0" border="0" style="font-size: 13px; color: #444444; line-height: 1.8;">
                                                                <tr><td width="85" style="color: #22BD60; font-weight: bold; padding-right: 10px;">Group:</td><td>${item.group}</td></tr>
                                                                ${item.unit ? `<tr><td style="color: #22BD60; font-weight: bold; padding-right: 10px;">Unit:</td><td>${item.unit}</td></tr>` : ''}
                                                                ${item.romp ? `<tr><td style="color: #22BD60; font-weight: bold; padding-right: 10px;">Romp:</td><td>${item.romp}</td></tr>` : ''}
                                                            </table>
                                                        </td>
                                                        <td align="right" width="200" style="vertical-align: top; padding-top: 5px;">
                                                            <div style="background-color: ${item.status === 'Initiated' ? '#fffbeb' : '#eff6ff'}; border: 1px solid ${item.status === 'Initiated' ? '#fde68a' : '#dbeafe'}; padding: 5px 12px; display: inline-block; margin-bottom: 10px;">
                                                                <span style="font-size: 10px; font-weight: bold; color: ${item.status === 'Initiated' ? '#b45309' : '#1e40af'}; text-transform: uppercase;">${item.status}</span>
                                                            </div>
                                                            <div style="font-size: 12px; color: #777777;"><span style="color: #22BD60; font-weight: bold;">Assigned:</span><br><span style="color: #111111; font-weight: bold; font-size: 13px;">${item.person}</span></div>
                                                        </td>
                                                    </tr>
                                                </table>
                                            </td>
                                        </tr>`;
                                    }).join('')}
                                </table>`;
                            }).join('')}
                        </td>
                    </tr>

                    <!-- Footer -->
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
                                        Monday.com Report System v2.1 &bull; 5 / 10 / 30-Day Periods
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

// ── Email Delivery (MS Graph preferred, SMTP fallback) ─────────────────────

function splitRecipients(value) {
    return String(value || '')
        .split(/[;,]/)
        .map(entry => entry.trim())
        .filter(Boolean);
}

function getReportRecipientValue() {
    return process.env.REPORT_TO_EMAIL || process.env.EMAIL_TO || '';
}

async function sendEmailIfConfigured(htmlContent, csvContent, csvFileName) {
    const recipientList = getReportRecipientValue();
    if (!recipientList) {
        console.log('📧 Email skipped: no REPORT_TO_EMAIL configured.');
        return;
    }

    if (process.env.M365_TENANT_ID && process.env.M365_CLIENT_ID && process.env.M365_CLIENT_SECRET && process.env.M365_SENDER_UPN) {
        await sendEmailViaGraph(htmlContent, csvContent, csvFileName);
        return;
    }

    if (!process.env.EMAIL_USER || !process.env.EMAIL_PASS) {
        console.log('📧 Email skipped: no Graph app config and no SMTP credentials configured.');
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

    await transporter.sendMail({
        from: `"Monday Report Bot" <${process.env.EMAIL_USER}>`,
        to: recipientList,
        subject: `Enclosure Support Report - ${new Date().toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' })}`,
        html: htmlContent,
        attachments: [{
            filename: csvFileName,
            content: csvContent
        }]
    });

    console.log(`📧 Email sent via SMTP: ${recipientList}`);
}

async function sendEmailViaGraph(htmlContent, csvContent, csvFileName) {
    const tenantId = process.env.M365_TENANT_ID;
    const clientId = process.env.M365_CLIENT_ID;
    const clientSecret = process.env.M365_CLIENT_SECRET;
    const senderUpn = process.env.M365_SENDER_UPN;
    const tokenUrl = `https://login.microsoftonline.com/${encodeURIComponent(tenantId)}/oauth2/v2.0/token`;

    const tokenRes = await fetch(tokenUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
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
                subject: `Enclosure Support Report - ${new Date().toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' })}`,
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

    console.log(`📧 Email sent via Microsoft Graph: ${recipientValue}`);
}

generateReport().catch(err => console.error('❌ Fatal Error:', err));
