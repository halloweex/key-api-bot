// Chart instances
let revenueChart = null;
let ordersChart = null;
let revenueBySourceChart = null;
let topProductsChart = null;
// Customer insights charts
let customersChart = null;
let aovTrendChart = null;
// Product performance charts
let categoryChart = null;
let topRevenueChart = null;
// Brand analytics charts
let brandRevenueChart = null;
let brandQuantityChart = null;

// Current filters
let currentPeriod = 'week';
let customStartDate = null;
let customEndDate = null;
let currentSourceId = null;
let currentParentCategoryId = null;
let currentCategoryId = null;
let currentBrand = null;

// Revenue milestones (UAH) - period-specific
const MILESTONES = {
    daily: [
        { amount: 200000, message: '200K Daily Revenue!', emoji: '🎉' },
    ],
    weekly: [
        { amount: 800000, message: '800K Weekly Revenue!', emoji: '🔥' },
        { amount: 1000000, message: '1 MILLION Weekly!', emoji: '🚀🎆' },
        { amount: 2000000, message: '2 MILLION Weekly!', emoji: '💎🎇' },
    ],
    monthly: [
        { amount: 10000000, message: '10 MILLION Monthly!', emoji: '👑🎇🎆' },
    ]
};
let lastCelebratedMilestone = {}; // Track per period type

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    initDateInputs();
    initPeriodButtons();
    initApplyButton();
    initSourceFilter();
    initCategoryFilter();
    initBrandFilter();
    initInfoTooltips();
    loadCategories();
    loadBrands();
    loadAllData();
});

// Update milestone progress bar
function updateMilestoneProgress(revenue) {
    const section = document.getElementById('milestoneSection');
    const fill = document.getElementById('milestoneFill');
    const percentage = document.getElementById('milestonePercentage');
    const target = document.getElementById('milestoneTarget');
    const current = document.getElementById('milestoneCurrent');
    const goal = document.getElementById('milestoneGoal');

    // Determine period type and get milestone
    let periodType = null;
    let periodLabel = '';
    if (currentPeriod === 'today' || currentPeriod === 'yesterday') {
        periodType = 'daily';
        periodLabel = 'Daily Goal';
    } else if (currentPeriod === 'week' || currentPeriod === 'last_week') {
        periodType = 'weekly';
        periodLabel = 'Weekly Goal';
    } else if (currentPeriod === 'month' || currentPeriod === 'last_month') {
        periodType = 'monthly';
        periodLabel = 'Monthly Goal';
    }

    // Hide for custom periods
    if (!periodType || !MILESTONES[periodType] || MILESTONES[periodType].length === 0) {
        section.classList.add('hidden');
        return;
    }

    section.classList.remove('hidden');

    // Get the first (primary) milestone for this period
    const milestones = MILESTONES[periodType];
    const primaryMilestone = milestones[0];
    const milestoneAmount = primaryMilestone.amount;

    // Calculate progress
    const progress = Math.min((revenue / milestoneAmount) * 100, 100);
    const exceeded = revenue >= milestoneAmount;

    // Format amounts
    const formatAmount = (amount) => {
        if (amount >= 1000000) {
            return `₴${(amount / 1000000).toFixed(1)}M`;
        } else if (amount >= 1000) {
            return `₴${(amount / 1000).toFixed(0)}K`;
        }
        return `₴${amount.toFixed(0)}`;
    };

    // Update UI
    target.textContent = periodLabel;
    fill.style.width = `${progress}%`;
    fill.classList.toggle('exceeded', exceeded);
    percentage.textContent = `${Math.round(progress)}%`;
    percentage.style.color = exceeded ? '#7C3AED' : '#16A34A';
    current.textContent = formatAmount(revenue);
    current.style.color = exceeded ? '#7C3AED' : '#16A34A';
    goal.textContent = formatAmount(milestoneAmount);

    // Add markers for additional milestones if any
    const markersContainer = document.getElementById('milestoneMarkers');
    markersContainer.innerHTML = '';

    if (milestones.length > 1) {
        milestones.slice(1).forEach(m => {
            const markerPos = (m.amount / milestoneAmount) * 100;
            if (markerPos <= 100) {
                const marker = document.createElement('div');
                marker.className = 'milestone-marker';
                marker.style.left = `${markerPos}%`;
                marker.dataset.label = formatAmount(m.amount);
                markersContainer.appendChild(marker);
            }
        });
    }
}

// Check and celebrate revenue milestones
function checkMilestones(revenue) {
    // Determine period type
    let periodType = null;
    if (currentPeriod === 'today' || currentPeriod === 'yesterday') {
        periodType = 'daily';
    } else if (currentPeriod === 'week' || currentPeriod === 'last_week') {
        periodType = 'weekly';
    } else if (currentPeriod === 'month' || currentPeriod === 'last_month') {
        periodType = 'monthly';
    }

    // No milestones for custom periods
    if (!periodType || !MILESTONES[periodType]) {
        return;
    }

    // Find the highest milestone reached for this period type
    let highestMilestone = null;
    for (const milestone of MILESTONES[periodType]) {
        if (revenue >= milestone.amount) {
            highestMilestone = milestone;
        }
    }

    // Initialize tracking for this period type
    if (!lastCelebratedMilestone[periodType]) {
        lastCelebratedMilestone[periodType] = 0;
    }

    // Only celebrate if we hit a new milestone (higher than last celebrated for this period)
    if (highestMilestone && highestMilestone.amount > lastCelebratedMilestone[periodType]) {
        lastCelebratedMilestone[periodType] = highestMilestone.amount;
        celebrate(highestMilestone);
    }
}

// Launch celebration with confetti
function celebrate(milestone) {
    // Format amount for display
    let amountText;
    if (milestone.amount >= 1000000) {
        amountText = `₴${(milestone.amount / 1000000).toFixed(1)}M`;
    } else {
        amountText = `₴${(milestone.amount / 1000).toFixed(0)}K`;
    }

    // Create celebration overlay
    const overlay = document.createElement('div');
    overlay.className = 'celebration-overlay';
    overlay.innerHTML = `
        <div class="celebration-message">
            <span class="celebration-emoji">${milestone.emoji}</span>
            <h2>${milestone.message}</h2>
            <p>Congratulations on reaching ${amountText}!</p>
        </div>
    `;
    document.body.appendChild(overlay);

    // Create confetti
    createConfetti();

    // Remove after animation
    setTimeout(() => {
        overlay.classList.add('fade-out');
        setTimeout(() => overlay.remove(), 500);
    }, 4000);

    // Click to dismiss
    overlay.addEventListener('click', () => {
        overlay.classList.add('fade-out');
        setTimeout(() => overlay.remove(), 500);
    });
}

// Create confetti particles
function createConfetti() {
    const colors = ['#2563EB', '#7C3AED', '#16A34A', '#F59E0B', '#DC2626', '#EC4899'];
    const confettiCount = 150;

    for (let i = 0; i < confettiCount; i++) {
        const confetti = document.createElement('div');
        confetti.className = 'confetti';
        confetti.style.left = Math.random() * 100 + 'vw';
        confetti.style.backgroundColor = colors[Math.floor(Math.random() * colors.length)];
        confetti.style.animationDuration = (Math.random() * 3 + 2) + 's';
        confetti.style.animationDelay = Math.random() * 2 + 's';
        document.body.appendChild(confetti);

        // Remove after animation
        setTimeout(() => confetti.remove(), 5000);
    }
}

// Initialize info tooltips
function initInfoTooltips() {
    // Customers info tooltip
    const customersInfoBtn = document.getElementById('customersInfoBtn');
    const customersInfoTooltip = document.getElementById('customersInfoTooltip');

    if (customersInfoBtn && customersInfoTooltip) {
        customersInfoBtn.addEventListener('click', function(e) {
            e.stopPropagation();
            customersInfoTooltip.classList.toggle('active');
        });

        // Close button
        const closeBtn = customersInfoTooltip.querySelector('.info-tooltip-close');
        if (closeBtn) {
            closeBtn.addEventListener('click', function() {
                customersInfoTooltip.classList.remove('active');
            });
        }

        // Close when clicking outside
        document.addEventListener('click', function(e) {
            if (!customersInfoTooltip.contains(e.target) && e.target !== customersInfoBtn) {
                customersInfoTooltip.classList.remove('active');
            }
        });
    }

    // AOV info tooltip
    const aovInfoBtn = document.getElementById('aovInfoBtn');
    const aovInfoTooltip = document.getElementById('aovInfoTooltip');

    if (aovInfoBtn && aovInfoTooltip) {
        aovInfoBtn.addEventListener('click', function(e) {
            e.stopPropagation();
            aovInfoTooltip.classList.toggle('active');
        });

        // Close button
        const aovCloseBtn = aovInfoTooltip.querySelector('.info-tooltip-close');
        if (aovCloseBtn) {
            aovCloseBtn.addEventListener('click', function() {
                aovInfoTooltip.classList.remove('active');
            });
        }

        // Close when clicking outside
        document.addEventListener('click', function(e) {
            if (!aovInfoTooltip.contains(e.target) && e.target !== aovInfoBtn) {
                aovInfoTooltip.classList.remove('active');
            }
        });
    }
}

// Load parent categories for filter dropdown
async function loadCategories() {
    try {
        const response = await fetch('/api/categories');
        const categories = await response.json();

        const select = document.getElementById('parentCategoryFilter');
        categories.forEach(cat => {
            const option = document.createElement('option');
            option.value = cat.id;
            option.textContent = cat.name;
            select.appendChild(option);
        });
    } catch (error) {
        console.error('Error loading categories:', error);
    }
}

// Load brands for filter dropdown
async function loadBrands() {
    try {
        const response = await fetch('/api/brands');
        const brands = await response.json();

        const select = document.getElementById('brandFilter');
        brands.forEach(brand => {
            const option = document.createElement('option');
            option.value = brand.name;
            option.textContent = brand.name;
            select.appendChild(option);
        });
    } catch (error) {
        console.error('Error loading brands:', error);
    }
}

// Source filter handler
function initSourceFilter() {
    document.getElementById('sourceFilter').addEventListener('change', function() {
        currentSourceId = this.value ? parseInt(this.value) : null;
        loadAllData();
    });
}

// Brand filter handler
function initBrandFilter() {
    document.getElementById('brandFilter').addEventListener('change', function() {
        currentBrand = this.value || null;
        loadAllData();
    });
}

// Load child categories for a parent
async function loadChildCategories(parentId) {
    const select = document.getElementById('categoryFilter');

    // Clear existing options
    select.innerHTML = '<option value="">All Subcategories</option>';

    if (!parentId) {
        select.disabled = true;
        return;
    }

    try {
        const response = await fetch(`/api/categories/${parentId}/children`);
        const categories = await response.json();

        if (categories.length > 0) {
            categories.forEach(cat => {
                const option = document.createElement('option');
                option.value = cat.id;
                option.textContent = cat.name;
                select.appendChild(option);
            });
            select.disabled = false;
        } else {
            select.disabled = true;
        }
    } catch (error) {
        console.error('Error loading child categories:', error);
        select.disabled = true;
    }
}

// Category filter handlers
function initCategoryFilter() {
    // Parent category change
    document.getElementById('parentCategoryFilter').addEventListener('change', function() {
        currentParentCategoryId = this.value ? parseInt(this.value) : null;
        currentCategoryId = null;
        document.getElementById('categoryFilter').value = '';
        loadChildCategories(currentParentCategoryId);
        loadAllData();
    });

    // Child category change
    document.getElementById('categoryFilter').addEventListener('change', function() {
        currentCategoryId = this.value ? parseInt(this.value) : null;
        loadAllData();
    });
}

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
    let query = '';

    if (currentPeriod) {
        query = `?period=${currentPeriod}`;
    } else if (customStartDate && customEndDate) {
        query = `?start_date=${customStartDate}&end_date=${customEndDate}`;
    } else {
        query = '?period=today';
    }

    // Add source filter
    if (currentSourceId) {
        query += `&source_id=${currentSourceId}`;
    }

    // Add category filter - use child category if selected, otherwise parent
    const effectiveCategoryId = currentCategoryId || currentParentCategoryId;
    if (effectiveCategoryId) {
        query += `&category_id=${effectiveCategoryId}`;
    }

    // Add brand filter
    if (currentBrand) {
        query += `&brand=${encodeURIComponent(currentBrand)}`;
    }

    return query;
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
            loadTopProducts(),
            loadCustomerInsights(),
            loadProductPerformance(),
            loadBrandAnalytics()
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

        // Update milestone progress bar
        updateMilestoneProgress(data.totalRevenue);

        // Check for milestone celebrations
        checkMilestones(data.totalRevenue);
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
                    },
                    datalabels: {
                        color: '#fff',
                        font: {
                            weight: 'bold',
                            size: 14
                        },
                        anchor: 'center',
                        align: 'center',
                        formatter: function(value) {
                            return value > 0 ? value : '';
                        }
                    }
                },
                scales: {
                    y: {
                        display: false
                    }
                }
            },
            plugins: [ChartDataLabels]
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
                    },
                    datalabels: {
                        color: '#fff',
                        font: {
                            weight: 'bold',
                            size: 12
                        },
                        formatter: function(value, context) {
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const percentage = ((value / total) * 100).toFixed(0);
                            return percentage > 5 ? percentage + '%' : '';
                        }
                    }
                }
            },
            plugins: [ChartDataLabels]
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
                    backgroundColor: data.backgroundColor,
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
                    },
                    datalabels: {
                        color: '#fff',
                        font: {
                            weight: 'bold',
                            size: 11
                        },
                        anchor: 'center',
                        align: 'center',
                        formatter: function(value, context) {
                            const percentage = data.percentages[context.dataIndex];
                            return value > 0 ? `${value} (${percentage}%)` : '';
                        }
                    }
                },
                scales: {
                    x: {
                        display: false
                    }
                }
            },
            plugins: [ChartDataLabels]
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

// Load customer insights
async function loadCustomerInsights() {
    try {
        const response = await fetch('/api/customers/insights' + buildQuery());
        const data = await response.json();

        // Update customer metrics cards
        document.getElementById('totalCustomers').textContent = data.metrics.totalCustomers.toLocaleString();
        document.getElementById('repeatRate').textContent = data.metrics.repeatRate + '%';
        document.getElementById('avgOrderValue').textContent = formatCurrency(data.metrics.averageOrderValue);

        // New vs Returning Customers pie chart
        const customersCtx = document.getElementById('customersChart').getContext('2d');

        if (customersChart) {
            customersChart.destroy();
        }

        customersChart = new Chart(customersCtx, {
            type: 'doughnut',
            data: {
                labels: data.newVsReturning.labels,
                datasets: [{
                    data: data.newVsReturning.data,
                    backgroundColor: data.newVsReturning.backgroundColor,
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
                    datalabels: {
                        color: '#fff',
                        font: {
                            weight: 'bold',
                            size: 14
                        },
                        formatter: function(value, context) {
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            if (total === 0) return '';
                            const percentage = ((value / total) * 100).toFixed(0);
                            return percentage > 5 ? value + '\n(' + percentage + '%)' : '';
                        }
                    }
                }
            },
            plugins: [ChartDataLabels]
        });

        // AOV Trend line chart
        const aovCtx = document.getElementById('aovTrendChart').getContext('2d');

        if (aovTrendChart) {
            aovTrendChart.destroy();
        }

        aovTrendChart = new Chart(aovCtx, {
            type: 'line',
            data: data.aovTrend,
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
        console.error('Error loading customer insights:', error);
    }
}

// Load product performance
async function loadProductPerformance() {
    try {
        const response = await fetch('/api/products/performance' + buildQuery());
        const data = await response.json();

        // Category breakdown pie chart
        const categoryCtx = document.getElementById('categoryChart').getContext('2d');

        if (categoryChart) {
            categoryChart.destroy();
        }

        categoryChart = new Chart(categoryCtx, {
            type: 'doughnut',
            data: {
                labels: data.categoryBreakdown.labels,
                datasets: [{
                    data: data.categoryBreakdown.revenue,
                    backgroundColor: data.categoryBreakdown.backgroundColor,
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
                                const qty = data.categoryBreakdown.quantity[context.dataIndex];
                                return `${context.label}: ${formatCurrency(value)} (${percentage}%) - ${qty} items`;
                            }
                        }
                    },
                    datalabels: {
                        color: '#fff',
                        font: {
                            weight: 'bold',
                            size: 11
                        },
                        formatter: function(value, context) {
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            if (total === 0) return '';
                            const percentage = ((value / total) * 100).toFixed(0);
                            return percentage > 5 ? percentage + '%' : '';
                        }
                    }
                }
            },
            plugins: [ChartDataLabels]
        });

        // Top products by revenue bar chart
        const revenueCtx = document.getElementById('topRevenueChart').getContext('2d');

        if (topRevenueChart) {
            topRevenueChart.destroy();
        }

        topRevenueChart = new Chart(revenueCtx, {
            type: 'bar',
            data: {
                labels: data.topByRevenue.labels,
                datasets: [{
                    label: 'Revenue',
                    data: data.topByRevenue.data,
                    backgroundColor: data.topByRevenue.backgroundColor,
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
                                const qty = data.topByRevenue.quantities[index];
                                return `Revenue: ${formatCurrency(context.parsed.x)} (${qty} sold)`;
                            }
                        }
                    },
                    datalabels: {
                        color: '#fff',
                        font: {
                            weight: 'bold',
                            size: 10
                        },
                        anchor: 'center',
                        align: 'center',
                        formatter: function(value, context) {
                            return value > 0 ? formatCurrency(value) : '';
                        }
                    }
                },
                scales: {
                    x: {
                        display: false
                    }
                }
            },
            plugins: [ChartDataLabels]
        });
    } catch (error) {
        console.error('Error loading product performance:', error);
    }
}

// Load brand analytics
async function loadBrandAnalytics() {
    try {
        // Build query without brand filter for brand analytics
        let query = '';
        if (currentPeriod) {
            query = `?period=${currentPeriod}`;
        } else if (customStartDate && customEndDate) {
            query = `?start_date=${customStartDate}&end_date=${customEndDate}`;
        } else {
            query = '?period=today';
        }

        const response = await fetch('/api/brands/analytics' + query);
        const data = await response.json();

        // Update brand metrics cards
        document.getElementById('totalBrands').textContent = data.metrics.totalBrands.toLocaleString();
        document.getElementById('topBrandName').textContent = data.metrics.topBrand;
        document.getElementById('topBrandShare').textContent = data.metrics.topBrandShare + '%';

        // Top brands by revenue bar chart
        const revenueCtx = document.getElementById('brandRevenueChart').getContext('2d');

        if (brandRevenueChart) {
            brandRevenueChart.destroy();
        }

        brandRevenueChart = new Chart(revenueCtx, {
            type: 'bar',
            data: {
                labels: data.topByRevenue.labels,
                datasets: [{
                    label: 'Revenue',
                    data: data.topByRevenue.data,
                    backgroundColor: data.topByRevenue.backgroundColor,
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
                                const qty = data.topByRevenue.quantities[index];
                                const orders = data.topByRevenue.orders[index];
                                return `Revenue: ${formatCurrency(context.parsed.x)} (${qty} items, ${orders} orders)`;
                            }
                        }
                    },
                    datalabels: {
                        color: '#fff',
                        font: {
                            weight: 'bold',
                            size: 10
                        },
                        anchor: 'center',
                        align: 'center',
                        formatter: function(value, context) {
                            return value > 0 ? formatCurrency(value) : '';
                        }
                    }
                },
                scales: {
                    x: {
                        display: false
                    }
                }
            },
            plugins: [ChartDataLabels]
        });

        // Top brands by quantity bar chart
        const quantityCtx = document.getElementById('brandQuantityChart').getContext('2d');

        if (brandQuantityChart) {
            brandQuantityChart.destroy();
        }

        brandQuantityChart = new Chart(quantityCtx, {
            type: 'bar',
            data: {
                labels: data.topByQuantity.labels,
                datasets: [{
                    label: 'Quantity',
                    data: data.topByQuantity.data,
                    backgroundColor: data.topByQuantity.backgroundColor,
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
                                const revenue = data.topByQuantity.revenue[index];
                                return `Quantity: ${context.parsed.x} (${formatCurrency(revenue)})`;
                            }
                        }
                    },
                    datalabels: {
                        color: '#fff',
                        font: {
                            weight: 'bold',
                            size: 11
                        },
                        anchor: 'center',
                        align: 'center',
                        formatter: function(value, context) {
                            return value > 0 ? value : '';
                        }
                    }
                },
                scales: {
                    x: {
                        display: false
                    }
                }
            },
            plugins: [ChartDataLabels]
        });
    } catch (error) {
        console.error('Error loading brand analytics:', error);
    }
}
