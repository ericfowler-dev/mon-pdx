// Shared configuration for Monday.com reports
module.exports = {
    // Monday.com workspace settings
    TARGET_WORKSPACE_ID: 10402502,
    MONDAY_SLUG: 'psiengines-company',

    // Boards to exclude from all reports
    EXCLUDED_BOARDS: [
        'Schedule',
        'Field Service Parts Request',
        'Site Locations and Contacts',
        'IMC TOR1',
        'Field Service Template',
        'IMC TOR 1 201089 Cost Only',
        'Shipping Process Proof of Concept',
        'Subcontracting Job Tracker',
        'TEST BOARD for adding issue problem,
    ],

    // Statuses considered "open" for tracking
    OPEN_STATUSES: ['Initiated', 'Open', 'In-Process', 'In Progress', 'New Issue'],

    // Statuses considered "closed" for throughput tracking (expanded per user feedback)
    CLOSED_STATUSES: ['Complete', 'Completed', 'Closed', 'Done', 'Resolved', 'Review Status', 'Pending', 'Cancelled', 'Non-Issue', 'CAR Pending', 'CX Alloy Update', 'CxA (Iconicx) Pending Verification', 'Review', 'Pending Job', 'Stack Verification'],

    // Comparison period in days (aligned with velocity metrics)
    COMPARISON_DAYS: 5,

    // Health score targets (used for weighted calculation)
    // OPEN_TARGET: Expected "healthy" backlog size per board. Lower = stricter scoring.
    // Boards with openItems >> OPEN_TARGET will score lower on backlog health.
    OPEN_TARGET: 30,

    // Aging thresholds in days
    AGING_THRESHOLDS: {
        WARNING: 30,   // Yellow - needs attention
        CRITICAL: 60,  // Orange - overdue
        SEVERE: 90     // Red - severely overdue
    },

    // Column IDs (stable across boards)
    COL_IDS: {
        STATUS: 'status',
        PERSON: 'person',
        CX_ALLOY: 'text_mkpcjgq6',
        UNIT: 'dropdown_mkpc31cs',
        ROMP: 'text_mkpc3vca',
        PRIORITY: 'priority',
        DATE_CLOSED: 'date_mksqfdbb'  // "Date of Review/Closure" column (corrected ID)
    },

    // Special responsibility rules for person report
    SPECIAL_RESPONSIBILITIES: {
        'Need to Order': { people: ['Ambrea Ayala'], override: false },
        'Part Unknown': { people: ['Shawn Strathmann'], override: true }
    },

    // Output settings
    HISTORY_RETENTION_DAYS: 30
};
