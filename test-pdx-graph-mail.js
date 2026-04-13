require('dotenv').config();

function splitRecipients(value) {
    return String(value || '')
        .split(/[;,]/)
        .map(entry => entry.trim())
        .filter(Boolean);
}

function getRequiredEnv(name) {
    const value = process.env[name];
    if (!value) {
        throw new Error(`Missing required environment variable: ${name}`);
    }

    return value;
}

function getRecipientValue() {
    return process.env.REPORT_TO_EMAIL || process.env.EMAIL_TO || '';
}

async function main() {
    const tenantId = getRequiredEnv('M365_TENANT_ID');
    const clientId = getRequiredEnv('M365_CLIENT_ID');
    const clientSecret = getRequiredEnv('M365_CLIENT_SECRET');
    const senderUpn = getRequiredEnv('M365_SENDER_UPN');
    const recipientValue = getRecipientValue();

    if (!recipientValue) {
        throw new Error('Missing REPORT_TO_EMAIL or EMAIL_TO.');
    }

    const recipients = splitRecipients(recipientValue).map(address => ({
        emailAddress: { address }
    }));

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
    if (!accessToken) {
        throw new Error('Graph token response did not include an access token.');
    }

    const now = new Date();
    const subjectTimestamp = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')} ${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;
    const html = `
        <html>
        <body style="font-family: Segoe UI, Arial, sans-serif;">
            <h2 style="margin-bottom: 8px;">PDX Graph Test</h2>
            <p style="margin: 0 0 8px 0;">This is a direct Microsoft Graph app-only mail test.</p>
            <p style="margin: 0;"><strong>Sent:</strong> ${subjectTimestamp}</p>
            <p style="margin: 8px 0 0 0;"><strong>Sender UPN:</strong> ${senderUpn}</p>
        </body>
        </html>`;

    const graphRes = await fetch(`https://graph.microsoft.com/v1.0/users/${encodeURIComponent(senderUpn)}/sendMail`, {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${accessToken}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            message: {
                subject: `PDX Graph Test - ${subjectTimestamp}`,
                body: {
                    contentType: 'HTML',
                    content: html
                },
                toRecipients: recipients
            },
            saveToSentItems: true
        })
    });

    if (!graphRes.ok) {
        const body = await graphRes.text();
        throw new Error(`Graph sendMail failed (${graphRes.status}): ${body}`);
    }

    console.log(`Graph test email sent to: ${recipientValue}`);
}

main().catch(error => {
    console.error(error.message);
    process.exitCode = 1;
});
