// Chart instances
let revenueChart = null;
let ordersChart = null;
let revenueBySourceChart = null;
let topProductsChart = null;

// Current period
let currentPeriod = 'week';
let customStartDate = null;
let customEndDate = null;

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    initDateInputs();
    initPeriodButtons();
    initApplyButton();
    loadAllData();
});

// Initialize date inputs with today's date
function initDateInputs() {
    const today = new Date().toISOString().split('T')[0];
    document.getElementById('startDate').value = today;
    document.getElementById('endDate').value = today;
}

// Period button handlers
function initPeriodButtons() {
    const buttons = document.querySelectorAll('.period-btn');
    buttons.forEach(btn => {
        btn.addEventListener('click', function() {
            buttons.forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            currentPeriod = this.dataset.period;
            customStartDate = null;
            customEndDate = null;
            loadAllData();
        });
    });
}

// Apply custom dates button
function initApplyButton() {
    document.getElementById('applyDates').addEventListener('click', function() {
        const start = document.getElementById('startDate').value;
        const end = document.getElementById('endDate').value;

        if (start && end) {
            document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
            currentPeriod = null;
            customStartDate = start;
            customEndDate = end;
            loadAllData();
        }
    });
}

// Build query string
function buildQuery() {
    if (currentPeriod) {
        return `?period=${currentPeriod}`;
    } else if (customStartDate && customEndDate) {
        return `?start_date=${customStartDate}&end_date=${customEndDate}`;
    }
    return '?period=today';
}

// Show/hide loading overlay
function showLoading() {
    document.getElementById('loadingOverlay').classList.add('active');
}

function hideLoading() {
    document.getElementById('loadingOverlay').classList.remove('active');
}

// Load all data
async function loadAllData() {
    showLoading();

    try {
        await Promise.all([
            loadSummary(),
            loadRevenueChart(),
            loadSalesCharts(),
            loadTopProducts()
        ]);
    } catch (error) {
        console.error('Error loading data:', error);
    } finally {
        hideLoading();
    }
}

// Load summary statistics
async function loadSummary() {
    try {
        const response = await fetch('/api/summary' + buildQuery());
        const data = await response.json();

        document.getElementById('totalOrders').textContent = data.totalOrders.toLocaleString();
        document.getElementById('totalRevenue').textContent = formatCurrency(data.totalRevenue);
        document.getElementById('avgCheck').textContent = formatCurrency(data.avgCheck);
        document.getElementById('totalReturns').textContent = data.totalReturns.toLocaleString();
    } catch (error) {
        console.error('Error loading summary:', error);
    }
}

// Load revenue trend chart
async function loadRevenueChart() {
    try {
        const response = await fetch('/api/revenue/trend' + buildQuery());
        const data = await response.json();

        const ctx = document.getElementById('revenueChart').getContext('2d');

        if (revenueChart) {
            revenueChart.destroy();
        }

        revenueChart = new Chart(ctx, {
            type: 'line',
            data: data,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return formatCurrency(value);
                            }
                        }
                    }
                },
                interaction: {
                    intersect: false,
                    mode: 'index'
                }
            }
        });
    } catch (error) {
        console.error('Error loading revenue chart:', error);
    }
}

// Load orders and revenue by source charts
async function loadSalesCharts() {
    try {
        const response = await fetch('/api/sales/by-source' + buildQuery());
        const data = await response.json();

        // Orders by source (bar chart)
        const ordersCtx = document.getElementById('ordersChart').getContext('2d');

        if (ordersChart) {
            ordersChart.destroy();
        }

        ordersChart = new Chart(ordersCtx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'Orders',
                    data: data.orders,
                    backgroundColor: data.backgroundColor,
                    borderRadius: 6
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });

        // Revenue by source (doughnut chart)
        const revenueCtx = document.getElementById('revenueBySourceChart').getContext('2d');

        if (revenueBySourceChart) {
            revenueBySourceChart.destroy();
        }

        revenueBySourceChart = new Chart(revenueCtx, {
            type: 'doughnut',
            data: {
                labels: data.labels,
                datasets: [{
                    data: data.revenue,
                    backgroundColor: data.backgroundColor,
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom'
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const value = context.parsed;
                                const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                const percentage = ((value / total) * 100).toFixed(1);
                                return `${context.label}: ${formatCurrency(value)} (${percentage}%)`;
                            }
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('Error loading sales charts:', error);
    }
}

// Load top products chart
async function loadTopProducts() {
    try {
        const response = await fetch('/api/products/top' + buildQuery());
        const data = await response.json();

        const ctx = document.getElementById('topProductsChart').getContext('2d');

        if (topProductsChart) {
            topProductsChart.destroy();
        }

        topProductsChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'Quantity',
                    data: data.data,
                    backgroundColor: '#3498db',
                    borderRadius: 4
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const index = context.dataIndex;
                                const percentage = data.percentages[index];
                                return `Quantity: ${context.parsed.x} (${percentage}%)`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true
                    }
                }
            }
        });
    } catch (error) {
        console.error('Error loading top products:', error);
    }
}

// Format currency
function formatCurrency(value) {
    return new Intl.NumberFormat('uk-UA', {
        style: 'currency',
        currency: 'UAH',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    }).format(value);
}
