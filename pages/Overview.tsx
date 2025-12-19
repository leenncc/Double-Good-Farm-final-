import React, { useEffect, useState } from 'react';
import { 
    fetchBatches, getFinishedGoods, getDailyProductionCosts, 
    getLaborRate, getRawMaterialRate, getSales
} from '../services/sheetService';
import { 
    MushroomBatch, BatchStatus, FinishedGood, DailyCostMetrics, SalesRecord 
} from '../types';
import { 
    Truck, Droplets, Package, TrendingUp, ArrowRight, Activity, 
    ClipboardList, DollarSign, Sprout, Clock, ArrowDown, FileText 
} from 'lucide-react';

const OverviewPage: React.FC = () => {
  const [batches, setBatches] = useState<MushroomBatch[]>([]);
  const [finishedGoods, setFinishedGoods] = useState<FinishedGood[]>([]);
  const [costs, setCosts] = useState<DailyCostMetrics[]>([]);
  const [sales, setSales] = useState<SalesRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [laborRate, setLaborRate] = useState(0);
  const [rawRate, setRawRate] = useState(0);

  useEffect(() => {
    const loadData = async () => {
      const [batchRes, goodsRes, costRes, salesRes] = await Promise.all([
          fetchBatches(), 
          getFinishedGoods(),
          getDailyProductionCosts(),
          getSales(true)
      ]);
      
      if (batchRes.success && batchRes.data) setBatches(batchRes.data);
      if (goodsRes.success && goodsRes.data) setFinishedGoods(goodsRes.data);
      if (costRes.success && costRes.data) {
        setCosts(costRes.data.sort((a,b) => new Date(b.date).getTime() - new Date(a.date).getTime()));
      }
      if (salesRes.success && salesRes.data) setSales(salesRes.data);
      
      setLaborRate(getLaborRate());
      setRawRate(getRawMaterialRate());
      setLoading(false);
    };
    loadData();
  }, []);

  // Calculate Metrics
  const receivedCount = batches.filter(b => b.status === BatchStatus.RECEIVED).length;
  const processingCount = batches.filter(b => b.status === BatchStatus.PROCESSING).length;
  const readyToPackCount = batches.filter(b => b.status === BatchStatus.DRYING_COMPLETE).length;
  const finishedCount = finishedGoods.reduce((acc, item) => acc + item.quantity, 0);
  const totalWeight = batches.reduce((acc, b) => acc + b.netWeightKg, 0);
  const totalPaidRevenue = sales.filter(s => s.status === 'PAID').reduce((acc, s) => acc + s.totalAmount, 0);

  const ProcessCard = ({ title, count, icon: Icon, color, desc, suffix, className }: any) => (
    <div className={`bg-white p-6 rounded-2xl shadow-sm border border-slate-200 flex flex-col items-center text-center relative overflow-hidden group hover:shadow-md transition-all ${className}`}>
      <div className={`p-4 rounded-full ${color} bg-opacity-10 mb-3 group-hover:scale-110 transition-transform duration-300`}>
        <Icon size={28} className={color.replace('bg-', 'text-')} />
      </div>
      <h3 className="text-lg font-bold text-slate-800">{title}</h3>
      <div className="text-4xl font-bold text-slate-900 my-2">{count} {suffix && <span className="text-sm text-slate-400 font-normal">{suffix}</span>}</div>
      <p className="text-xs text-slate-500">{desc}</p>
    </div>
  );

  const ArrowDivider = () => (
    <div className="flex items-center justify-center text-slate-300">
      <div className="hidden md:block"><ArrowRight size={24} /></div>
      <div className="md:hidden"><ArrowDown size={24} /></div>
    </div>
  );

  return (
    <div className="space-y-8">
      <div>
        <h2 className="text-3xl font-bold text-earth-900">Operations Overview</h2>
        <p className="text-earth-600">Live production pipeline monitoring</p>
      </div>

      {/* Process Pipeline Visual */}
      <div className="flex flex-col md:flex-row gap-4 items-center justify-between">
        <ProcessCard title="Receiving" count={receivedCount} icon={Truck} color="bg-blue-500 text-blue-600" desc="Batches waiting for wash" suffix="Batches" className="w-full flex-1" />
        <ArrowDivider />
        <ProcessCard title="Processing" count={processingCount} icon={Droplets} color="bg-orange-500 text-orange-600" desc="Currently washing/drying" suffix="Batches" className="w-full flex-1" />
        <ArrowDivider />
        <ProcessCard title="Packing" count={readyToPackCount} icon={Package} color="bg-purple-500 text-purple-600" desc="Dried & ready to label" suffix="Batches" className="w-full flex-1" />
        <ArrowDivider />
        <ProcessCard title="Finished Stock" count={finishedCount} icon={TrendingUp} color="bg-green-500 text-green-600" desc="Units ready for sale" suffix="Units" className="w-full flex-1" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Recent Activity Feed */}
        <div className="lg:col-span-2 bg-white rounded-2xl shadow-sm border border-slate-200 p-6">
          <div className="flex items-center justify-between mb-6">
            <h3 className="font-bold text-slate-800 flex items-center">
              <Activity size={20} className="mr-2 text-nature-600" />
              Recent Batch Activity
            </h3>
            <span className="text-xs font-medium text-slate-400">Live Updates</span>
          </div>
          
          <div className="space-y-4">
            {loading ? (
              <p className="text-slate-400 text-center py-4">Syncing with Master Log...</p>
            ) : batches.slice(0, 5).map((batch) => (
              <div key={batch.id} className="flex items-center justify-between p-3 hover:bg-slate-50 rounded-lg transition-colors border-b border-slate-50 last:border-0">
                <div className="flex items-center space-x-3">
                  <div className={`w-2 h-2 rounded-full ${
                    batch.status === BatchStatus.RECEIVED ? 'bg-blue-500' :
                    batch.status === BatchStatus.PROCESSING ? 'bg-orange-500' :
                    batch.status === BatchStatus.DRYING_COMPLETE ? 'bg-purple-500' :
                    'bg-green-500'
                  }`} />
                  <div>
                    <p className="font-medium text-slate-700 text-sm">{batch.id} - {batch.sourceFarm}</p>
                    <p className="text-xs text-slate-400">{new Date(batch.dateReceived).toLocaleDateString()}</p>
                  </div>
                </div>
                <div className="text-right">
                  <span className="text-xs font-semibold px-2 py-1 bg-slate-100 rounded text-slate-600">{batch.status}</span>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Quick Stats */}
        <div className="bg-earth-800 rounded-2xl p-6 text-white flex flex-col justify-between relative overflow-hidden">
          <div className="relative z-10">
            <h3 className="text-earth-200 font-medium mb-1">Total Net Weight Processed</h3>
            <div className="text-4xl font-bold mb-4">{totalWeight.toFixed(1)} <span className="text-lg text-earth-400">kg</span></div>
            <div className="mt-8 space-y-2">
              <div className="flex justify-between text-sm text-earth-300"><span>System Health</span><span>100%</span></div>
              <div className="w-full bg-earth-700 rounded-full h-1.5"><div className="bg-nature-400 h-1.5 rounded-full" style={{ width: '100%' }}></div></div>
            </div>
          </div>
          <div className="absolute -bottom-10 -right-10 text-earth-700 opacity-20"><TrendingUp size={150} /></div>
        </div>
      </div>

      {/* REVENUE HISTORY SECTION */}
      <div className="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden">
          <div className="p-4 bg-green-50 border-b border-green-100 flex justify-between items-center">
              <h3 className="font-bold text-green-900 flex items-center">
                  <TrendingUp className="mr-2 text-green-600" /> Revenue History
              </h3>
              <span className="text-xs text-green-700 font-bold bg-green-100 px-2 py-1 rounded">
                  Verified Income (Paid Only)
              </span>
          </div>
          <div className="overflow-x-auto">
              <table className="w-full text-sm text-left">
                  <thead className="bg-slate-50 text-slate-500 font-bold border-b border-slate-200">
                      <tr>
                          <th className="p-3">Date</th>
                          <th className="p-3">Invoice ID</th>
                          <th className="p-3">Customer</th>
                          <th className="p-3">Items Sold</th>
                          <th className="p-3 text-center">Status</th>
                          <th className="p-3 text-right">Amount</th>
                      </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                      {sales.filter(s => s.status === 'PAID').length === 0 ? (
                          <tr><td colSpan={6} className="p-8 text-center text-slate-400 italic">No paid transactions recorded yet.</td></tr>
                      ) : (
                          sales
                          .filter(s => s.status === 'PAID') 
                          .sort((a, b) => new Date(b.dateCreated).getTime() - new Date(a.dateCreated).getTime())
                          .map(sale => (
                              <tr key={sale.id} className="hover:bg-green-50/50 transition-colors">
                                  <td className="p-3 font-medium text-slate-700">{new Date(sale.dateCreated).toLocaleDateString()}</td>
                                  <td className="p-3 font-mono text-xs text-slate-500">{sale.invoiceId || sale.id}</td>
                                  <td className="p-3 text-slate-700 font-bold">{sale.customerName}</td>
                                  <td className="p-3 text-xs text-slate-600 max-w-xs truncate">
                                      {sale.items.map(i => `${i.quantity}x ${i.recipeName}`).join(', ')}
                                  </td>
                                  <td className="p-3 text-center">
                                      <span className="text-[10px] font-bold px-2 py-0.5 rounded uppercase bg-green-100 text-green-700">
                                          {sale.status}
                                      </span>
                                  </td>
                                  <td className="p-3 text-right font-black text-green-700">
                                      RM {sale.totalAmount.toFixed(2)}
                                  </td>
                              </tr>
                          ))
                      )}
                  </tbody>
                  <tfoot className="bg-slate-50 border-t border-slate-200">
                      <tr>
                          <td colSpan={5} className="p-3 text-right font-bold text-slate-500 uppercase text-xs">Total Paid Revenue</td>
                          <td className="p-3 text-right font-black text-slate-800 text-lg">
                              RM {totalPaidRevenue.toFixed(2)}
                          </td>
                      </tr>
                  </tfoot>
              </table>
          </div>
      </div>

      {/* PRODUCTION COST LOG */}
      <div className="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden">
          <div className="p-6 border-b border-slate-100 flex flex-col md:flex-row md:items-center justify-between gap-4">
              <h3 className="font-bold text-slate-800 text-lg flex items-center">
                  <FileText size={20} className="mr-2 text-slate-600" />
                  Daily Production Cost Log
              </h3>
              <div className="text-xs text-slate-400">Combined view per batch</div>
          </div>
          <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                  <thead className="bg-slate-50 text-slate-500 font-bold border-b border-slate-200">
                      <tr>
                          <th className="px-4 py-3">Date</th>
                          <th className="px-4 py-3">Ref ID</th>
                          <th className="px-4 py-3">Activity</th>
                          <th className="px-4 py-3 text-right">Raw Mat.</th>
                          <th className="px-4 py-3 text-right">Packing</th>
                          <th className="px-4 py-3 text-right">Labor</th>
                          <th className="px-4 py-3 text-right">Wastage</th>
                          <th className="px-4 py-3 text-right">Total</th>
                      </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                      {costs.length === 0 ? (
                          <tr><td colSpan={8} className="p-8 text-center text-slate-400">No cost logs recorded.</td></tr>
                      ) : (
                          costs.map((cost) => (
                              <tr key={cost.id} className="hover:bg-slate-50 transition-colors">
                                  <td className="px-4 py-4 text-slate-700 font-medium">{new Date(cost.date).toLocaleDateString()}</td>
                                  <td className="px-4 py-4 text-xs font-mono text-blue-600">{cost.referenceId}</td>
                                  <td className="px-4 py-4 text-slate-500 text-xs">
                                      {cost.weightProcessed > 0 ? `Proc: ${cost.weightProcessed}kg` : '-'}
                                  </td>
                                  <td className="px-4 py-4 text-right text-slate-700">
                                      {cost.rawMaterialCost > 0 ? `RM ${cost.rawMaterialCost.toFixed(2)}` : 'RM 0.00'}
                                  </td>
                                  <td className="px-4 py-4 text-right text-slate-700">
                                      {cost.packagingCost > 0 ? `RM ${cost.packagingCost.toFixed(2)}` : 'RM 0.00'}
                                  </td>
                                  <td className="px-4 py-4 text-right text-slate-700">
                                      {cost.laborCost > 0 ? `RM ${cost.laborCost.toFixed(2)}` : 'RM 0.00'}
                                  </td>
                                  <td className="px-4 py-4 text-right text-red-500 font-medium">
                                      {cost.wastageCost > 0 ? `RM ${cost.wastageCost.toFixed(2)}` : 'RM 0.00'}
                                  </td>
                                  <td className="px-4 py-4 text-right font-black text-slate-900">
                                      RM {cost.totalCost.toFixed(2)}
                                  </td>
                              </tr>
                          ))
                      )}
                  </tbody>
              </table>
          </div>
      </div>
    </div>
  );
};

export default OverviewPage;