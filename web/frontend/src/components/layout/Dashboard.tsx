import { SummaryCards } from '../cards'
import {
  RevenueTrendChart,
  SalesBySourceChart,
  TopProductsChart,
  CategoryChart,
} from '../charts'

export function Dashboard() {
  return (
    <main className="p-6 space-y-6">
      {/* Summary Cards */}
      <section>
        <SummaryCards />
      </section>

      {/* Charts Grid - Row 1 */}
      <section className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <RevenueTrendChart />
        <SalesBySourceChart />
      </section>

      {/* Charts Grid - Row 2 */}
      <section className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <TopProductsChart />
        <CategoryChart />
      </section>
    </main>
  )
}
