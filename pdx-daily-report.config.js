module.exports = {
    TARGET_WORKSPACE_ID: 10402502,
    MONDAY_SLUG: 'psiengines-company',
    REPORT_NAME: 'PDX Daily Report',
    BOARD_NAME: 'PDX (Dunder Mifflin)',

    EXCLUDED_GROUPS: ['hub'],

    OPEN_STATUSES: ['Initiated', 'Open', 'In-Process', 'In Progress', 'New Issue'],
    CLOSED_STATUSES: [
        'Complete',
        'Completed',
        'Closed',
        'Done',
        'Resolved',
        'Review Status',
        'Pending',
        'Cancelled',
        'Non-Issue',
        'CAR Pending',
        'CX Alloy Update',
        'CxA (Iconicx) Pending Verification',
        'Review',
        'Pending Job',
        'Stack Verification'
    ],

    COMPARISON_DAYS: 7,
    RECENT_CLOSED_DAYS: 10,
    RECENT_SECTION_STATUSES: ['Closed', 'Pending', 'CX Alloy Update'],
    OPEN_TARGET: 20,

    AGING_THRESHOLDS: {
        WARNING: 30,
        CRITICAL: 60,
        SEVERE: 90
    },

    COL_IDS: {
        STATUS: 'status',
        PERSON: 'person',
        FIELD_TECH: 'multiple_person_mkrdhetc',
        CX_ALLOY: 'text_mkpcjgq6',
        UNIT: 'dropdown_mkpc31cs',
        ROMP: 'text_mkpc3vca',
        PRIORITY: 'color_mkpcjpy7',
        DATE_CLOSED: 'date_mksmamvk'
    },

    HISTORY_RETENTION_DAYS: 90
};
