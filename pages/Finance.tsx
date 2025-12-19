
import React, { useEffect, useState, useMemo } from 'react';
import { 
    getFinishedGoods, getInventory, getPurchaseOrders, createPurchaseOrder, 
    receivePurchaseOrder, complaintPurchaseOrder, resolveComplaint, getSuppliers, 
    addSupplier, addInventoryItem, getCustomers, addCustomer, 
    createSale, updateSaleStatus, getSales, getDailyProductionCosts, updateDailyCost, 
    getWeeklyRevenue, getLaborRate, setLaborRate, getRawMaterialRate, setRawMaterialRate,
    getMonthlyBudget, setMonthlyBudget 
} from '../services/sheetService';
import { InventoryItem, PurchaseOrder, Customer, FinishedGood, SalesRecord, DailyCostMetrics, Supplier, SalesStatus, Budget } from '../types';
import { ShoppingCart, AlertTriangle, CheckCircle2, Truck, Plus, Trash2, Building2, TrendingUp, PieChart, Store, FileText, Send, Printer, User, Pencil, Clock, Sprout, FileClock, PackageCheck, Receipt, Loader2, Target, ChevronDown, ChevronUp, X, Settings, ShoppingBag, RefreshCw, Wallet, XCircle, LayoutList, Calendar, ClipboardList, RotateCcw, FileMinus, FileSearch } from 'lucide-react';

interface FinanceProps {
  allowedTabs?: string[]; 
}

// Added interface to resolve type errors on availableGoods items
interface AvailableGoodItem {
  key: string;
  id: string;
  label: string;
  totalQty: number;
  price: number;
}

type DocumentType = 'QUOTATION' | 'INVOICE' | 'DO' | 'RECEIPT' | 'CANCELLATION';

const FinancePage: React.FC<FinanceProps> = ({ allowedTabs = ['procurement', 'sales', 'overview'] }) => {
  const [activeTab, setActiveTab] = useState<'overview' | 'procurement' | 'sales'>(() => {
    if (allowedTabs.includes('procurement')) return 'procurement';
    if (allowedTabs.includes('sales')) return 'sales';
    return 'overview';
  });

  const [inventory, setInventory] = useState<InventoryItem[]>([]);
  const [purchaseOrders, setPurchaseOrders] = useState<PurchaseOrder[]>([]);
  const [suppliers, setSuppliers] = useState<Supplier[]>([]);
  const [customers, setCustomers] = useState<Customer[]>([]);
  const [finishedGoods, setFinishedGoods] = useState<FinishedGood[]>([]);
  const [sales, setSales] = useState<SalesRecord[]>([]);
  const [dailyCosts, setDailyCosts] = useState<DailyCostMetrics[]>([]);
  const [weeklyRevenue, setWeeklyRevenue] = useState<{date: string, amount: number}[]>([]);
  const [currentBudget, setCurrentBudget] = useState<Budget | null>(null);
  const [showBudgetForm, setShowBudgetForm] = useState(false);
  const [targetRevenue, setTargetRevenue] = useState('');
  const [targetProfit, setTargetProfit] = useState('');
  const [laborRate, setLaborRateState] = useState<number>(12.50);
  const [rawRate, setRawMaterialRateState] = useState<number>(8.00);
  const [showReportModal, setShowReportModal] = useState(false);
  const [showCancelModal, setShowCancelModal] = useState<string | null>(null);
  const [cancelReason, setCancelReason] = useState('Customer changed mind');
  const [customCancelReason, setCustomCancelReason] = useState('');
  const [showOrderModal, setShowOrderModal] = useState(false);
  const [showQCModal, setShowQCModal] = useState<string | null>(null);
  const [showComplaintModal, setShowComplaintModal] = useState<string | null>(null);
  const [selectedSale, setSelectedSale] = useState<SalesRecord | null>(null);
  const [viewDocType, setViewDocType] = useState<DocumentType>('INVOICE');
  const [showSupplierModal, setShowSupplierModal] = useState(false);
  const [showCustomerModal, setShowCustomerModal] = useState(false);
  const [showRateModal, setShowRateModal] = useState(false);
  const [overviewView, setOverviewView] = useState<'COST' | 'REVENUE'>('COST');
  const [poItem, setPoItem] = useState('');
  const [poQtyPackages, setPoQtyPackages] = useState('1');
  const [complaintReason, setComplaintReason] = useState('');
  
  // New state for Order Record view
  const [viewingOrder, setViewingOrder] = useState<PurchaseOrder | null>(null);
  
  const [newSupplier, setNewSupplier] = useState({ 
    name: '', address: '', contact: '', itemName: '', itemType: 'PACKAGING', itemSubtype: 'POUCH', packSize: 100, unitCost: 45 
  });
  
  const [newCustomer, setNewCustomer] = useState<Partial<Customer>>({ name: '', email: '', contact: '', address: '', type: 'B2C', status: 'ACTIVE' });
  const [salesCustomer, setSalesCustomer] = useState('');
  const [salesGood, setSalesGood] = useState('');
  const [salesQty, setSalesQty] = useState('1');
  const [salesPayment, setSalesPayment] = useState<'CASH' | 'COD' | 'CREDIT_CARD'>('CASH');
  const [salesPrice, setSalesPrice] = useState('15.00');
  const [salesCart, setSalesCart] = useState<{id: string, label: string, qty: number, price: number}[]>([]);
  const [updatingId, setUpdatingId] = useState<string | null>(null);
  const [hoveredPieIndex, setHoveredPieIndex] = useState<number | null>(null);
  const [reportPeriod, setReportPeriod] = useState<'WEEK' | 'MONTH'>('MONTH');

  const refreshData = async () => {
    setLaborRateState(getLaborRate());
    setRawMaterialRateState(getRawMaterialRate());
    const [s, inv, po, sup, cust, goods, costs, rev] = await Promise.all([
        getSales(true), getInventory(), getPurchaseOrders(), getSuppliers(), getCustomers(), 
        getFinishedGoods(), getDailyProductionCosts(), getWeeklyRevenue()
    ]);
    if (s.success) setSales(s.data || []);
    if (inv.success) setInventory(inv.data || []);
    if (po.success) setPurchaseOrders(po.data || []);
    if (sup.success) setSuppliers(sup.data || []);
    if (cust.success) setCustomers(cust.data || []);
    if (goods.success) setFinishedGoods(goods.data || []);
    if (costs.success) setDailyCosts(costs.data || []);
    setWeeklyRevenue(rev);
    const budgetRes = await getMonthlyBudget(new Date().toISOString().slice(0, 7));
    if (budgetRes.success && budgetRes.data) {
        setCurrentBudget(budgetRes.data); setTargetRevenue(budgetRes.data.targetRevenue.toString()); setTargetProfit(budgetRes.data.targetProfit.toString());
    }
  };

  useEffect(() => { refreshData(); }, [activeTab]);

  // Typed availableGoods to resolve 'unknown' member issues in JSX
  const availableGoods = finishedGoods.reduce((acc, curr) => {
     const key = `${curr.recipeName}|${curr.packagingType}`;
     if (!acc[key]) acc[key] = { key: key, id: curr.id, label: `${curr.recipeName} (${curr.packagingType})`, totalQty: 0, price: curr.sellingPrice || 15 };
     acc[key].totalQty += curr.quantity;
     return acc;
  }, {} as Record<string, AvailableGoodItem>);

  useEffect(() => { if (salesGood && availableGoods[salesGood]) setSalesPrice(availableGoods[salesGood].price.toFixed(2)); }, [salesGood, finishedGoods]);

  const aggregatedCosts = useMemo(() => {
      const map = new Map<string, DailyCostMetrics>();
      dailyCosts.forEach(cost => {
          const key = `${cost.date}|${cost.referenceId}`;
          if (!map.has(key)) map.set(key, { ...cost });
          else {
              const ex = map.get(key)!; ex.weightProcessed += cost.weightProcessed || 0; ex.processingHours += cost.processingHours || 0;
              ex.rawMaterialCost += cost.rawMaterialCost || 0; ex.packagingCost += cost.packagingCost || 0; ex.laborCost += cost.laborCost || 0;
              ex.wastageCost += cost.wastageCost || 0; ex.totalCost += cost.totalCost || 0;
          }
      });
      return Array.from(map.values()).sort((a,b) => new Date(b.date).getTime() - new Date(a.date).getTime());
  }, [dailyCosts]);

  const totalPackagingProcurement = purchaseOrders.filter(p => p.status === 'RECEIVED' || p.status === 'ORDERED').reduce((acc, p) => acc + p.totalCost, 0);
  const totalRawMaterialCost = dailyCosts.reduce((acc, d) => acc + d.rawMaterialCost, 0);
  const totalLaborCost = dailyCosts.reduce((acc, d) => acc + d.laborCost, 0);
  const totalWastageCost = dailyCosts.reduce((acc, d) => acc + d.wastageCost, 0);
  const totalOverallCost = totalPackagingProcurement + totalRawMaterialCost + totalLaborCost + totalWastageCost;
  const totalSalesRevenue = sales.filter(s => s.status === 'PAID').reduce((acc, s) => acc + s.totalAmount, 0);
  const netProfit = totalSalesRevenue - totalOverallCost;
  const avgCostPerUnit = finishedGoods.reduce((acc, i) => acc + i.quantity, 0) > 0 ? totalOverallCost / finishedGoods.reduce((acc, i) => acc + i.quantity, 0) : 0;
  const revenueProgress = currentBudget && currentBudget.targetRevenue > 0 ? (totalSalesRevenue / currentBudget.targetRevenue) * 100 : 0;
  const profitProgress = currentBudget && currentBudget.targetProfit > 0 ? (netProfit / currentBudget.targetProfit) * 100 : 0;

  const reportStats = useMemo(() => {
    const days = reportPeriod === 'WEEK' ? 7 : 30;
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const periodSales = sales.filter(s => s.status === 'PAID' && new Date(s.dateCreated) >= cutoff);
    const periodCosts = dailyCosts.filter(c => new Date(c.date) >= cutoff);
    const periodPOs = purchaseOrders.filter(p => p.status === 'RECEIVED' && new Date(p.dateReceived || p.dateOrdered) >= cutoff);
    const revenue = periodSales.reduce((sum, s) => sum + s.totalAmount, 0);
    const rawCost = periodCosts.reduce((sum, c) => sum + (c.rawMaterialCost || 0), 0);
    const laborCost = periodCosts.reduce((sum, c) => sum + (c.laborCost || 0), 0);
    const wasteCost = periodCosts.reduce((sum, c) => sum + (c.wastageCost || 0), 0);
    const pkgCost = periodPOs.reduce((sum, p) => sum + p.totalCost, 0);
    const totalExp = rawCost + laborCost + wasteCost + pkgCost;
    return { revenue, rawCost, laborCost, wasteCost, pkgCost, totalExp, profit: revenue - totalExp, count: periodSales.length, aov: periodSales.length > 0 ? revenue / periodSales.length : 0, margin: revenue > 0 ? ((revenue - totalExp) / revenue) * 100 : 0, startDate: cutoff.toLocaleDateString(), endDate: new Date().toLocaleDateString() };
  }, [reportPeriod, sales, dailyCosts, purchaseOrders]);

  const handleAddToCart = () => {
      if (!salesGood) return;
      const product = availableGoods[salesGood];
      if (!product || parseInt(salesQty) <= 0) return;
      setSalesCart(prev => {
          const ex = prev.find(item => item.id === product.id);
          if (ex) return prev.map(item => item.id === product.id ? { ...item, qty: item.qty + parseInt(salesQty) } : item);
          return [...prev, { id: product.id, label: product.label, qty: parseInt(salesQty), price: parseFloat(salesPrice) }];
      });
      setSalesGood(''); setSalesQty('1');
  };

  const handleCreateDocument = async (e: React.FormEvent, initialStatus: 'QUOTATION' | 'INVOICED') => {
    e.preventDefault();
    if (salesCart.length === 0 || !salesCustomer) { alert("Missing data!"); return; }
    const itemsToSell = salesCart.map(item => ({ finishedGoodId: item.id, quantity: item.qty, unitPrice: item.price }));
    const res = await createSale(salesCustomer, itemsToSell, salesPayment, initialStatus);
    if (res.success && res.data) { setViewDocType(initialStatus === 'QUOTATION' ? 'QUOTATION' : 'INVOICE'); setSelectedSale(res.data); setSalesCart([]); refreshData(); } else alert(res.message);
  };

  const handleUpdateStatus = async (sale: SalesRecord, newStatus: string) => {
      setUpdatingId(sale.id);
      try {
          const res = await updateSaleStatus(sale.id, newStatus as SalesStatus);
          if (res.success && res.data) {
              setSales(prev => prev.map(s => s.id === sale.id ? { ...s, status: newStatus as SalesStatus } : s));
              if (selectedSale?.id === sale.id) setSelectedSale({ ...selectedSale, status: newStatus as SalesStatus });
              if (newStatus === 'INVOICED') setViewDocType('INVOICE');
              if (newStatus === 'SHIPPED') setViewDocType('DO');
              if (newStatus === 'PAID') setViewDocType('RECEIPT');
              refreshData();
          } else alert("Update failed: " + res.message);
      } catch (e: any) { alert("Error: " + e.message); } finally { setUpdatingId(null); }
  };

  const handleCancelSale = async () => {
      if (!showCancelModal) return;
      const finalReason = cancelReason === 'Others' ? customCancelReason : cancelReason;
      if (!finalReason) { alert("Reason required."); return; }
      try {
          const res = await updateSaleStatus(showCancelModal, 'CANCELLED' as any, { cancellationReason: finalReason });
          if (res.success) { setSales(prev => prev.map(s => s.id === showCancelModal ? { ...s, status: 'CANCELLED' as any, cancellationReason: finalReason } as any : s)); refreshData(); } else alert("Error: " + res.message);
          setShowCancelModal(null); setCancelReason('Customer changed mind'); setCustomCancelReason('');
      } catch (e) { alert("Error cancelling order."); }
  };

  const handleSaveBudget = async (e: React.FormEvent) => {
      e.preventDefault();
      const currentMonth = new Date().toISOString().slice(0, 7);
      await setMonthlyBudget({ id: currentMonth, month: currentMonth, targetRevenue: parseFloat(targetRevenue) || 0, targetProfit: parseFloat(targetProfit) || 0, maxWastageKg: 50 });
      setShowBudgetForm(false); refreshData();
  };

  const handleAddCustomer = async (e: React.FormEvent) => { 
      e.preventDefault(); 
      const res = await addCustomer({ id: `cust-${Date.now()}`, name: newCustomer.name || 'Unknown', email: newCustomer.email || '', contact: newCustomer.contact || '', address: newCustomer.address || '', type: newCustomer.type || 'B2C', status: 'ACTIVE', joinDate: new Date().toISOString() } as Customer); 
      if (res.success) { setShowCustomerModal(false); setNewCustomer({ name: '', email: '', contact: '', address: '', type: 'B2C' }); await refreshData(); }
  };

  const handleAddSupplier = async (e: React.FormEvent) => {
      e.preventDefault();
      const res = await addSupplier({ id: `sup-${Date.now()}`, name: newSupplier.name, address: newSupplier.address, contact: newSupplier.contact });
      if (res.success) {
          await addInventoryItem({ id: `inv-${Date.now()}`, name: newSupplier.itemName, type: newSupplier.itemType as any, subtype: newSupplier.itemSubtype as any, quantity: 0, threshold: 50, unit: 'units', unitCost: Number(newSupplier.unitCost), supplier: newSupplier.name, packSize: Number(newSupplier.packSize) });
          setShowSupplierModal(false); setNewSupplier({ name: '', address: '', contact: '', itemName: '', itemType: 'PACKAGING', itemSubtype: 'POUCH', packSize: 100, unitCost: 45 }); refreshData();
      }
  };

  const handleCreatePO = async (e: React.FormEvent) => { e.preventDefault(); const item = inventory.find(i => i.id === poItem); if (!item) return; await createPurchaseOrder(item.id, parseInt(poQtyPackages), item.supplier || 'Generic'); setShowOrderModal(false); refreshData(); };
  const handleQC = async (passed: boolean) => { if (showQCModal) { if (passed) await receivePurchaseOrder(showQCModal, true); else { setShowComplaintModal(showQCModal); setShowQCModal(null); return; } setShowQCModal(null); refreshData(); } };
  const handleSubmitComplaint = async () => { if (showComplaintModal && complaintReason) { await complaintPurchaseOrder(showComplaintModal, complaintReason); setShowComplaintModal(null); setComplaintReason(''); refreshData(); } };
  const handleOpenDocument = (sale: SalesRecord, type: DocumentType) => { setSelectedSale(sale); setViewDocType(type); };

  const handleResolveComplaint = async (id: string, resolution: string) => {
    // If resolution is "Refund processed", the prompt requests the UI show "CANCELLED".
    // sheetService resolveComplaint currently sets status to "RESOLVED"
    await resolveComplaint(id, resolution);
    refreshData();
  };

  const slices = (() => {
    let cumulativePercent = 0;
    const total = totalOverallCost;
    const data = [ { label: 'Raw Materials', color: '#15803d', cost: totalRawMaterialCost }, { label: 'Packaging', color: '#16a34a', cost: totalPackagingProcurement }, { label: 'Labor', color: '#3b82f6', cost: totalLaborCost }, { label: 'Wastage', color: '#ef4444', cost: totalWastageCost } ];
    return data.map(d => ({ ...d, pct: total > 0 ? (d.cost / total) * 100 : 0 })).filter(d => d.pct > 0).map((s, i) => {
        const x = Math.cos(2 * Math.PI * cumulativePercent); const y = Math.sin(2 * Math.PI * cumulativePercent);
        cumulativePercent += s.pct / 100;
        const x2 = Math.cos(2 * Math.PI * cumulativePercent); const y2 = Math.sin(2 * Math.PI * cumulativePercent);
        return { ...s, pathData: s.pct > 99.9 ? "M 1 0 A 1 1 0 1 1 -1 0 A 1 1 0 1 1 1 0" : `M 0 0 L ${x} ${y} A 1 1 0 ${s.pct / 100 > 0.5 ? 1 : 0} 1 ${x2} ${y2} L 0 0`, index: i };
    });
  })();

  const lowStockItems = inventory.filter(i => i.type !== 'FINISHED_GOOD' && i.quantity <= (i.threshold || 0));
  const purchaseableItems = useMemo(() => {
      return [...inventory].filter(i => i.type !== 'FINISHED_GOOD').sort((a, b) => {
          const aUrgent = a.quantity <= (a.threshold || 0); const bUrgent = b.quantity <= (b.threshold || 0);
          if (aUrgent && !bUrgent) return -1; if (!aUrgent && bUrgent) return 1; return a.name.localeCompare(b.name);
      });
  }, [inventory]);

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center bg-white p-4 rounded-xl shadow-sm border border-slate-100">
         <h2 className="text-xl font-bold text-slate-800">Operations</h2>
         <div className="flex bg-slate-100 p-1 rounded-lg">
           {['overview', 'procurement', 'sales'].map(t => allowedTabs.includes(t) && <button key={t} onClick={() => setActiveTab(t as any)} className={`px-4 py-2 capitalize rounded-md text-sm font-bold ${activeTab === t ? 'bg-white shadow text-slate-900' : 'text-slate-500'}`}>{t}</button>)}
         </div>
      </div>

      {activeTab === 'overview' && (
         <div className="space-y-6">
            <div className="flex justify-between items-center mb-4">
                <div><h2 className="text-2xl font-bold text-slate-800">Financial Overview</h2><p className="text-slate-500 text-sm">Real-time performance metrics</p></div>
                <button onClick={() => setShowReportModal(true)} className="flex items-center gap-2 px-4 py-2 bg-slate-800 text-white rounded-lg font-bold shadow-md hover:bg-slate-700 transition-all shadow-slate-200"><Printer size={18} /> Generate Report</button>
            </div>
            
            <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
                <div className="p-4 bg-slate-800 text-white flex justify-between items-center cursor-pointer" onClick={() => setShowBudgetForm(!showBudgetForm)}><div className="flex items-center"><Target className="mr-2 text-green-400" /><div><h3 className="font-bold text-lg">Financial Planning</h3><p className="text-xs text-slate-400">Monthly Targets</p></div></div>{showBudgetForm ? <ChevronUp /> : <ChevronDown />}</div>
                {showBudgetForm && <div className="p-6 bg-slate-50 border-b border-slate-200 animate-in slide-in-from-top-2"><form onSubmit={handleSaveBudget} className="flex gap-4 items-end"><div><label className="block text-xs font-bold text-slate-500 mb-1">Target Revenue (RM)</label><input type="number" className="p-2 border rounded w-40" value={targetRevenue} onChange={e => setTargetRevenue(e.target.value)} /></div><div><label className="block text-xs font-bold text-slate-500 mb-1">Target Profit (RM)</label><input type="number" className="p-2 border rounded w-40" value={targetProfit} onChange={e => setTargetProfit(e.target.value)} /></div><button className="px-4 py-2 bg-blue-600 text-white rounded font-bold hover:bg-blue-700">Save Targets</button></form></div>}
                <div className="p-6 grid grid-cols-1 md:grid-cols-2 gap-8">
                    <div><div className="flex justify-between mb-2"><span className="text-sm font-bold text-slate-600">Revenue Goal</span><span className="text-sm font-bold text-slate-800">RM {totalSalesRevenue.toFixed(0)} / {currentBudget?.targetRevenue || '-'}</span></div><div className="w-full bg-slate-100 rounded-full h-4 overflow-hidden"><div className={`h-full transition-all duration-1000 ${revenueProgress < 50 ? 'bg-red-500' : 'bg-green-500'}`} style={{ width: `${Math.min(revenueProgress, 100)}%` }}></div></div></div>
                    <div><div className="flex justify-between mb-2"><span className="text-sm font-bold text-slate-600">Profit Goal</span><span className="text-sm font-bold text-slate-800">RM {netProfit.toFixed(0)} / {currentBudget?.targetProfit || '-'}</span></div><div className="w-full bg-slate-100 rounded-full h-4 overflow-hidden"><div className="h-full transition-all duration-1000 bg-blue-500" style={{ width: `${Math.min(profitProgress, 100)}%` }}></div></div></div>
                </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
               <div className="bg-white p-4 rounded-xl shadow-sm border border-slate-200"><p className="text-xs font-bold text-slate-400 uppercase">Total Revenue</p><p className="text-2xl font-black text-green-700">RM {totalSalesRevenue.toFixed(2)}</p></div>
               <div className="bg-white p-4 rounded-xl shadow-sm border border-slate-200"><p className="text-xs font-bold text-slate-400 uppercase">Total Expenses</p><p className="text-2xl font-black text-red-700">RM {totalOverallCost.toFixed(2)}</p></div>
               <div className="bg-white p-4 rounded-xl shadow-sm border border-slate-200"><p className={`text-xs font-bold uppercase ${netProfit >= 0 ? 'text-slate-400' : 'text-red-400'}`}>Net Profit</p><p className={`text-2xl font-black ${netProfit >= 0 ? 'text-blue-700' : 'text-red-700'}`}>RM {netProfit.toFixed(2)}</p></div>
               <div className="bg-white p-4 rounded-xl shadow-sm border border-slate-200"><p className="text-xs font-bold text-slate-400 uppercase">Avg Cost/Unit</p><p className="text-2xl font-black text-slate-700">RM {avgCostPerUnit.toFixed(2)}</p></div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div className="bg-white p-6 rounded-2xl shadow-sm border border-slate-200 flex flex-col items-center justify-center relative">
                    <div className="w-full flex justify-between items-start mb-4">
                        <h3 className="text-lg font-bold text-slate-800 flex items-center"><PieChart className="mr-2 text-slate-600" size={20}/> Expense Distribution</h3>
                        <button onClick={() => setShowRateModal(true)} className="text-[10px] font-bold bg-slate-100 hover:bg-slate-200 px-2 py-1 rounded flex items-center gap-1 border border-slate-200 text-slate-600 transition-colors"><Settings size={12}/> Rates</button>
                    </div>
                    <div className="relative w-64 h-64">
                        <svg viewBox="-1.2 -1.2 2.4 2.4" className="w-full h-full transform -rotate-90">{slices.map((s, i) => (<path key={i} d={s.pathData} fill={s.color} className="cursor-pointer transition-all hover:opacity-80" onMouseEnter={() => setHoveredPieIndex(i)} onMouseLeave={() => setHoveredPieIndex(null)} stroke="white" strokeWidth="0.02" />))}</svg>
                        <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none text-center">{hoveredPieIndex !== null ? (<><span className="text-[10px] font-black text-slate-400 uppercase tracking-widest">{slices[hoveredPieIndex].label}</span><span className="text-2xl font-black text-slate-800 block">{slices[hoveredPieIndex].pct.toFixed(1)}%</span></>) : (<><span className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Total Exp.</span><span className="text-xl font-black text-slate-800 block">RM {totalOverallCost.toFixed(2)}</span></>)}</div>
                    </div>
                </div>
                <div className="bg-white p-6 rounded-2xl shadow-sm border border-slate-200">
                    <h3 className="text-lg font-bold text-slate-800 mb-6">Expense Details</h3>
                    <div className="space-y-4">{slices.map((s, i) => (<div key={i} className="flex items-center justify-between p-4 rounded-xl bg-slate-50 border border-slate-100 transition-all hover:bg-white hover:shadow-sm"><div className="flex items-center"><div className="w-3 h-3 rounded-full mr-3 shadow-sm" style={{ backgroundColor: s.color }}></div><span className="text-sm font-bold text-slate-700">{s.label}</span></div><div className="text-right"><p className="text-sm font-black text-slate-800">RM {s.cost.toFixed(2)}</p><p className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{s.pct.toFixed(1)}%</p></div></div>))}<div className="pt-4 mt-2 border-t border-slate-100 flex justify-between items-center"><span className="text-xs font-black text-slate-500 uppercase tracking-widest">TOTAL EXPENSES</span><span className="text-2xl font-black text-slate-900">RM {totalOverallCost.toFixed(2)}</span></div></div>
                </div>
            </div>

            <div className="flex items-center gap-3 mb-2 mt-8">
                <button onClick={() => setOverviewView('COST')} className={`px-5 py-2.5 rounded-lg font-bold text-sm flex items-center gap-2 transition-all ${overviewView === 'COST' ? 'bg-slate-800 text-white shadow-md' : 'bg-white text-slate-500 border border-slate-200'}`}><FileText size={18} /> Production Cost Log</button>
                <button onClick={() => setOverviewView('REVENUE')} className={`px-5 py-2.5 rounded-lg font-bold text-sm flex items-center gap-2 transition-all ${overviewView === 'REVENUE' ? 'bg-nature-600 text-white shadow-md' : 'bg-white text-slate-500 border border-slate-200'}`}><TrendingUp size={18} /> Revenue History</button>
            </div>

            {overviewView === 'COST' && (
              <div className="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden">
                <div className="p-4 bg-slate-50 border-b border-slate-100 flex flex-col md:flex-row md:items-center justify-between gap-4"><h3 className="font-bold text-slate-800 flex items-center"><FileText size={20} className="mr-2 text-slate-600" /> Daily Production Cost Log</h3><div className="text-xs text-slate-400 font-bold uppercase tracking-widest">Combined view per batch</div></div>
                <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead className="bg-slate-100 text-slate-500 font-bold border-b border-slate-200"><tr><th className="px-4 py-4">Date</th><th className="px-4 py-4">Ref ID</th><th className="px-4 py-4">Activity</th><th className="px-4 py-4 text-right">Raw Mat.</th><th className="px-4 py-4 text-right">Packing</th><th className="px-4 py-4 text-right">Labor</th><th className="px-4 py-4 text-right">Wastage</th><th className="px-4 py-4 text-right">Total</th></tr></thead><tbody className="divide-y divide-slate-100">{aggregatedCosts.map(c => (<tr key={c.id} className="hover:bg-blue-50 transition-colors"><td className="px-4 py-4 text-slate-700 font-medium">{new Date(c.date).toLocaleDateString()}</td><td className="px-4 py-4 text-xs font-mono text-slate-400">{c.referenceId}</td><td className="px-4 py-4 text-slate-500 text-xs">{c.weightProcessed > 0 ? `Proc: ${c.weightProcessed}kg` : c.processingHours > 0 ? `Labor: ${c.processingHours}h` : '-'}</td><td className="px-4 py-4 text-right text-slate-700 font-bold">RM {c.rawMaterialCost.toFixed(2)}</td><td className="px-4 py-4 text-right text-slate-700 font-bold">RM {c.packagingCost.toFixed(2)}</td><td className="px-4 py-4 text-right text-slate-700 font-bold">RM {c.laborCost.toFixed(2)}</td><td className="px-4 py-4 text-right text-red-500 font-bold">RM {c.wastageCost.toFixed(2)}</td><td className="px-4 py-4 text-right font-black text-slate-900">RM {c.totalCost.toFixed(2)}</td></tr>))}</tbody></table></div>
              </div>
            )}

            {overviewView === 'REVENUE' && (
              <div className="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden">
                <div className="p-4 bg-nature-50 border-b border-nature-100 flex justify-between items-center"><h3 className="font-bold text-nature-900 flex items-center"><TrendingUp className="mr-2 text-nature-600" /> Revenue History</h3><span className="text-[10px] text-nature-700 font-black bg-nature-100 px-2 py-1 rounded-md border border-nature-200 uppercase">Verified Income (Paid Only)</span></div>
                <div className="overflow-x-auto"><table className="w-full text-sm text-left"><thead className="bg-slate-50 text-slate-500 font-bold border-b border-slate-200"><tr><th className="p-4">Date</th><th className="p-4">Invoice ID</th><th className="p-4">Customer</th><th className="p-4">Items Sold</th><th className="p-4 text-center">Status</th><th className="p-4 text-right">Amount</th></tr></thead><tbody className="divide-y divide-slate-100">{sales.filter(s => s.status === 'PAID').map(s => (<tr key={s.id} className="hover:bg-nature-50/50 transition-colors"><td className="p-4 font-medium text-slate-700">{new Date(s.dateCreated).toLocaleDateString()}</td><td className="p-4 font-mono text-xs text-slate-400">{s.invoiceId}</td><td className="p-4 text-slate-800 font-black">{s.customerName}</td><td className="p-4 text-xs text-slate-600 max-w-xs truncate">{s.items.map(i => `${i.quantity}x ${i.recipeName}`).join(', ')}</td><td className="p-4 text-center"><span className="text-[10px] font-black px-2 py-1 rounded bg-nature-100 text-nature-700 border border-nature-200">PAID</span></td><td className="p-4 text-right font-black text-nature-700 text-lg">RM {s.totalAmount.toFixed(2)}</td></tr>))}</tbody><tfoot className="bg-slate-50 border-t border-slate-200"><tr><td colSpan={5} className="p-4 text-right font-black text-slate-500 uppercase text-xs tracking-widest">Total Paid Revenue</td><td className="p-4 text-right font-black text-slate-900 text-2xl">RM {totalSalesRevenue.toFixed(2)}</td></tr></tfoot></table></div>
              </div>
            )}
         </div>
      )}

      {activeTab === 'procurement' && (
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-8 animate-in fade-in">
           <div className="lg:col-span-3 space-y-8">
              {/* --- URGENT BANNER --- */}
              {lowStockItems.length > 0 && (
                <div className="bg-red-50 border border-red-200 rounded-[2rem] p-8 flex items-center justify-between shadow-sm">
                    <div className="flex items-center gap-6"><div className="w-16 h-16 bg-red-100 text-red-600 rounded-full flex items-center justify-center"><AlertTriangle size={32} strokeWidth={2.5} /></div><div><h3 className="text-red-900 font-black text-xl mb-1 uppercase tracking-tight">URGENT: Low Stock Alert</h3><ul className="space-y-1">{lowStockItems.map(item => (<li key={item.id} className="text-red-700 font-bold flex items-center"><span className="w-1.5 h-1.5 bg-red-700 rounded-full mr-2"></span>{item.name}: {item.quantity} {item.unit} left</li>))}</ul></div></div>
                    <button onClick={() => { setPoItem(lowStockItems[0].id); setShowOrderModal(true); }} className="px-8 py-4 bg-red-600 hover:bg-red-700 text-white rounded-2xl font-black flex items-center gap-3 shadow-xl shadow-red-200 transition-all"><ShoppingCart size={24} /> Reorder Now</button>
                </div>
              )}

              {/* ACTION BUTTONS */}
              <div className="flex justify-end gap-3"><button onClick={() => setShowSupplierModal(true)} className="px-6 py-3 bg-slate-100 text-slate-700 rounded-xl font-black flex items-center gap-2 hover:bg-slate-200 transition-all"><LayoutList size={20} /> Manage Suppliers</button><button onClick={() => setShowOrderModal(true)} className="px-6 py-3 bg-slate-800 text-white rounded-xl font-black flex items-center gap-2 hover:bg-black shadow-lg transition-all"><ShoppingCart size={20} /> New Order</button></div>

              {/* ACTIVE ORDERS SECTION */}
              <div className="space-y-4"><h3 className="font-black text-slate-800 text-lg uppercase tracking-tight">Active Orders</h3>{purchaseOrders.filter(p => p.status === 'ORDERED').length === 0 ? (<div className="py-12 border-2 border-dashed border-slate-200 rounded-3xl text-center text-slate-400 font-bold">No pending orders.</div>) : (<div className="space-y-3">{purchaseOrders.filter(p => p.status === 'ORDERED').map(po => (<div key={po.id} className="bg-white p-6 rounded-2xl shadow-sm border border-slate-100 flex justify-between items-center group hover:border-blue-200 transition-all"><div className="flex items-center gap-4"><div className="w-12 h-12 bg-blue-50 text-blue-600 rounded-xl flex items-center justify-center"><Truck size={24} /></div><div><div className="flex items-center gap-2 mb-0.5"><span className="text-[10px] font-black bg-blue-100 text-blue-700 px-2 py-0.5 rounded uppercase tracking-widest">{po.supplier}</span><span className="text-[10px] text-slate-400 font-bold uppercase">{po.id}</span></div><h4 className="font-black text-slate-800 text-lg">{po.itemName}</h4><p className="text-sm text-slate-500 font-bold uppercase">{po.quantity} packs • RM {po.totalCost.toFixed(2)}</p></div></div><button onClick={() => setShowQCModal(po.id)} className="px-6 py-3 bg-nature-600 text-white rounded-xl font-black shadow-lg shadow-nature-100 hover:bg-nature-700 transition-all">Received</button></div>))}</div>)}</div>

              {/* --- PENDING COMPLAINTS --- */}
              <div className="space-y-4"><h3 className="font-black text-red-800 text-lg uppercase tracking-tight flex items-center gap-2"><AlertTriangle size={20} /> Pending Complaints</h3>{purchaseOrders.filter(p => p.status === 'COMPLAINT').length === 0 ? (<div className="py-8 text-center text-slate-300 font-bold italic">No pending complaints.</div>) : (<div className="space-y-3">{purchaseOrders.filter(p => p.status === 'COMPLAINT').map(po => (<div key={po.id} className="bg-red-50/30 border border-red-100 rounded-2xl p-6 transition-all hover:bg-red-50/50"><div className="flex justify-between items-start mb-4"><div><div className="flex items-center gap-3 mb-2"><span className="text-[10px] font-black bg-white text-slate-500 border border-slate-200 px-2 py-1 rounded uppercase tracking-widest shadow-sm">{po.supplier}</span><span className="text-[10px] text-red-400 font-bold font-mono">#{po.id}</span></div><h4 className="text-xl font-black text-red-900 leading-tight">{po.itemName}</h4><div className="mt-2 inline-block px-3 py-1 bg-red-100/50 rounded-lg border border-red-200"><p className="text-xs text-red-700 font-black uppercase tracking-tight">Issue: {po.complaintReason}</p></div></div><div className="flex gap-2"><button onClick={() => handleResolveComplaint(po.id, 'Replacement received')} className="px-5 py-3 bg-white text-nature-700 border border-nature-200 rounded-xl font-black flex items-center gap-2 shadow-sm hover:bg-nature-50 transition-all"><RotateCcw size={18} /> Replacement</button><button onClick={() => handleResolveComplaint(po.id, 'Refund processed')} className="px-5 py-3 bg-white text-slate-700 border border-slate-200 rounded-xl font-black flex items-center gap-2 shadow-sm hover:bg-slate-50 transition-all"><FileMinus size={18} /> Refund</button></div></div></div>))}</div>)}</div>

              {/* --- REDESIGNED ORDER HISTORY (MATCHES IMAGE) --- */}
              <div className="space-y-4">
                  <h3 className="font-black text-slate-800 text-lg uppercase tracking-tight flex items-center gap-2">
                    <ClipboardList size={20} /> Order History
                  </h3>
                  {purchaseOrders.filter(p => p.status === 'RECEIVED' || p.status === 'RESOLVED').length === 0 ? (
                      <div className="py-12 border-2 border-dashed border-slate-100 rounded-3xl text-center text-slate-300 font-bold">No past orders found.</div>
                  ) : (
                      <div className="space-y-4">
                          {purchaseOrders.filter(p => p.status === 'RECEIVED' || p.status === 'RESOLVED').map(po => {
                              // If resolution was refund, show as cancelled per prompt
                              const isRefund = po.complaintResolution?.includes('Refund');
                              return (
                              <div key={po.id} className="bg-white rounded-3xl border border-slate-100 p-6 flex justify-between items-center shadow-sm hover:shadow-md transition-all">
                                  <div>
                                      <div className="flex items-center gap-3 mb-2">
                                          <span className={`text-[10px] font-black px-2 py-0.5 rounded border uppercase ${isRefund ? 'bg-red-50 text-red-600 border-red-100' : 'bg-blue-50 text-blue-600 border-blue-100'}`}>
                                              {isRefund ? 'CANCELLED' : 'RESOLVED'}
                                          </span>
                                          <span className="text-[10px] text-slate-400 font-bold uppercase tracking-widest font-mono">#{po.id} • {new Date(po.dateReceived || po.dateOrdered).toLocaleDateString()}</span>
                                      </div>
                                      <h4 className="font-black text-slate-800 text-xl">{po.itemName}</h4>
                                      <p className="text-sm text-slate-500 font-bold">Supplier: {po.supplier} • {po.quantity} packs</p>
                                  </div>
                                  <div className="text-right">
                                      <p className="text-xl font-black text-slate-900 mb-1">RM {po.totalCost.toFixed(2)}</p>
                                      <button 
                                        onClick={() => setViewingOrder(po)}
                                        className="text-blue-600 font-black text-sm hover:underline"
                                      >
                                          View Record
                                      </button>
                                  </div>
                              </div>
                          )})}
                      </div>
                  )}
              </div>
           </div>

           {/* --- SUPPLIER DIRECTORY SIDEBAR --- */}
           <div className="space-y-6"><div className="bg-white p-8 rounded-[2rem] shadow-sm border border-slate-100 h-fit"><h3 className="font-black text-slate-800 text-lg mb-6 uppercase tracking-tight">Supplier Directory</h3>{suppliers.length === 0 ? (<p className="text-slate-300 font-bold text-sm italic">No suppliers listed.</p>) : (<div className="divide-y divide-slate-50">{suppliers.map(s => (<div key={s.id} className="py-4 first:pt-0 last:pb-0"><h4 className="font-black text-slate-800 leading-none mb-1">{s.name}</h4><p className="text-xs text-slate-400 font-bold tracking-widest">{s.contact || 'No contact info'}</p></div>))}</div>)}</div></div>
        </div>
      )}

      {/* --- ORDER RECORD DETAIL MODAL (MATCHES IMAGE 3) --- */}
      {viewingOrder && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-sm p-4 animate-in fade-in duration-200">
              <div className="bg-white rounded-3xl shadow-2xl max-w-sm w-full overflow-hidden transform transition-all animate-in zoom-in-95">
                  <div className="p-6 border-b border-slate-100 bg-[#1e293b] text-white flex justify-between items-center">
                      <div className="flex items-center gap-2">
                        <FileSearch size={20} />
                        <h3 className="font-black text-lg">Order Record</h3>
                      </div>
                      <button onClick={() => setViewingOrder(null)} className="text-slate-400 hover:text-white transition-colors"><X size={24} /></button>
                  </div>

                  <div className="p-10 text-center">
                      <p className="text-[10px] font-black text-slate-400 uppercase tracking-[0.2em] mb-2">Total Value</p>
                      <h2 className="text-4xl font-black text-slate-900 mb-1">RM {viewingOrder.totalCost.toFixed(2)}</h2>
                      <p className="text-slate-400 font-bold text-xs mb-8">{new Date(viewingOrder.dateReceived || viewingOrder.dateOrdered).toLocaleDateString()}</p>

                      <div className="text-left space-y-4 border-t border-slate-100 pt-6">
                          <div className="flex justify-between items-center"><span className="text-slate-400 font-bold text-sm uppercase">Order ID</span><span className="font-mono text-sm font-black text-slate-800">#{viewingOrder.id}</span></div>
                          <div className="flex justify-between items-center"><span className="text-slate-400 font-bold text-sm uppercase">Item</span><span className="font-black text-slate-800">{viewingOrder.itemName}</span></div>
                          <div className="flex justify-between items-center"><span className="text-slate-400 font-bold text-sm uppercase">Supplier</span><span className="font-black text-slate-800">{viewingOrder.supplier}</span></div>
                      </div>

                      {viewingOrder.complaintReason && (
                          <div className="mt-8 p-6 bg-slate-50 border border-slate-100 rounded-[1.5rem] text-left">
                              <div className="flex items-center gap-2 mb-3"><AlertTriangle size={14} className="text-orange-500" /><span className="text-[10px] font-black text-slate-800 uppercase tracking-widest">Incident Report</span></div>
                              <div className="space-y-3">
                                  <div><p className="text-[9px] font-black text-slate-400 uppercase mb-1">Issue Reported:</p><p className="text-red-500 font-bold text-sm">{viewingOrder.complaintReason}</p></div>
                                  <div><p className="text-[9px] font-black text-slate-400 uppercase mb-1">Resolution:</p><p className="text-nature-700 font-black text-sm uppercase">{viewingOrder.complaintResolution || 'Pending'}</p></div>
                              </div>
                          </div>
                      )}
                  </div>

                  <div className="p-6 bg-slate-50 border-t border-slate-100">
                      <button 
                        onClick={() => setViewingOrder(null)} 
                        className="w-full py-4 bg-white border border-slate-200 text-slate-600 rounded-xl font-black shadow-sm hover:bg-slate-50 transition-all"
                      >
                          Close Record
                      </button>
                  </div>
              </div>
          </div>
      )}

      {activeTab === 'sales' && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 animate-in fade-in"><div className="bg-white p-6 rounded-xl border border-slate-200 shadow-sm h-fit"><h3 className="font-bold text-slate-800 mb-4 flex items-center"><Store className="mr-2"/> Point of Sale</h3><div className="space-y-4"><div><label className="block text-xs font-bold text-slate-500 mb-1">Customer</label><div className="flex space-x-2"><select className="w-full p-3 border rounded-lg bg-slate-50" value={salesCustomer} onChange={e => setSalesCustomer(e.target.value)}><option value="">Select Customer...</option>{customers.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}</select><button onClick={() => setShowCustomerModal(true)} className="p-3 bg-nature-600 text-white rounded-lg"><Plus size={18}/></button></div></div><div className="p-4 bg-slate-50 rounded-lg border border-slate-100 space-y-3"><select className="w-full p-2 border rounded-lg bg-white" value={salesGood} onChange={e => setSalesGood(e.target.value)}><option value="">Select Product...</option>{Object.values(availableGoods).map((g: AvailableGoodItem) => (<option key={g.key} value={g.key}>{g.label} (Stock: {g.totalQty})</option>))}</select><div className="flex items-center gap-2"><input type="number" min="1" className="w-20 p-2.5 border rounded-lg font-bold" value={salesQty} onChange={e => setSalesQty(e.target.value)} /><button onClick={handleAddToCart} className="flex-1 py-2.5 bg-earth-800 text-white rounded-lg font-bold">Add to Cart</button></div></div><div className="border-t border-b border-slate-100 py-2 max-h-40 overflow-y-auto">{salesCart.length === 0 ? <p className="text-center text-slate-400 text-sm py-4">Cart empty</p> : salesCart.map((it, i) => (<div key={i} className="flex justify-between items-center py-1 text-sm"><div><span className="font-bold text-slate-700">{it.qty}x</span> {it.label}</div><div className="flex items-center gap-2"><span>RM {(it.qty * it.price).toFixed(2)}</span><button onClick={() => setSalesCart(prev => prev.filter((_, idx) => idx !== i))} className="text-red-400"><X size={14}/></button></div></div>))}</div>{salesCart.length > 0 && <div className="flex justify-between items-center font-bold text-lg"><span>Total:</span><span>RM {salesCart.reduce((sum, i) => sum + (i.qty * i.price), 0).toFixed(2)}</span></div>}<div className="grid grid-cols-2 gap-3 pt-2"><button onClick={(e) => handleCreateDocument(e, 'QUOTATION')} className="w-full py-3 border-2 rounded-xl font-bold">Create Quote</button><button onClick={(e) => handleCreateDocument(e, 'INVOICED')} className="w-full py-3 bg-nature-600 text-white rounded-xl font-bold">Charge Invoice</button></div></div></div><div className="lg:col-span-2 space-y-4"><h3 className="font-bold text-slate-700 mb-2">Sales Ledger</h3>{sales.map(sale => (<div key={sale.id} className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm flex flex-col md:flex-row justify-between items-center gap-4"><div className="flex items-center space-x-4 w-full md:w-auto"><div className={`p-3 rounded-full flex-shrink-0 ${sale.status === 'QUOTATION' ? 'bg-purple-100 text-purple-600' : sale.status === 'PAID' ? 'bg-green-100 text-green-600' : 'bg-blue-100 text-blue-600'}`}>{sale.status === 'QUOTATION' ? <FileClock size={20}/> : sale.status === 'PAID' ? <CheckCircle2 size={20}/> : <FileText size={20}/>}</div><div><div className="flex items-center gap-2"><h4 className="font-bold text-slate-800">{sale.customerName}</h4><span className="text-[10px] font-bold px-2 py-0.5 rounded bg-slate-100 text-slate-500 uppercase">{sale.status}</span></div><p className="text-xs text-slate-500">RM {sale.totalAmount.toFixed(2)} • {sale.items.length} items</p></div></div><div className="flex flex-wrap gap-2 justify-end">{sale.status === 'QUOTATION' && <button onClick={() => handleUpdateStatus(sale, 'INVOICED')} disabled={updatingId === sale.id} className="px-3 py-1.5 bg-nature-600 text-white text-xs font-bold rounded hover:bg-nature-700 flex items-center disabled:opacity-50">{updatingId === sale.id ? <Loader2 size={12} className="animate-spin mr-1"/> : null} Confirm & Invoice</button>}{sale.status === 'INVOICED' && <button onClick={() => handleUpdateStatus(sale, 'SHIPPED')} disabled={updatingId === sale.id} className="px-3 py-1.5 bg-blue-600 text-white text-xs font-bold rounded hover:bg-blue-700 flex items-center disabled:opacity-50">{updatingId === sale.id ? <Loader2 size={12} className="animate-spin mr-1"/> : null} Generate DO</button>}{sale.status === 'SHIPPED' && <button onClick={() => handleUpdateStatus(sale, 'PAID')} disabled={updatingId === sale.id} className="px-3 py-1.5 bg-green-600 text-white text-xs font-bold rounded hover:bg-green-700 flex items-center disabled:opacity-50">{updatingId === sale.id ? <Loader2 size={12} className="animate-spin mr-1"/> : null} Mark Paid</button>}{(sale.status === 'QUOTATION' || sale.status === 'INVOICED') && <button onClick={() => setShowCancelModal(sale.id)} className="px-3 py-1.5 bg-red-50 text-red-600 text-xs font-bold rounded hover:bg-red-100 flex items-center border border-red-200"><XCircle size={12} className="mr-1"/> Cancel</button>}<div className="flex border-l pl-2 ml-2 space-x-1">{sale.status === 'QUOTATION' && <button onClick={() => handleOpenDocument(sale, 'QUOTATION')} className="p-1.5 text-slate-400 hover:bg-slate-100 rounded" title="View Quote"><FileClock size={16}/></button>}{sale.status !== 'QUOTATION' && sale.status !== 'CANCELLED' && <button onClick={() => handleOpenDocument(sale, 'INVOICE')} className="p-1.5 text-slate-400 hover:bg-slate-100 rounded" title="View Invoice"><FileText size={16}/></button>}{sale.status === 'PAID' && <button onClick={() => handleOpenDocument(sale, 'RECEIPT')} className="p-1.5 text-green-600 hover:bg-green-50 rounded" title="View Receipt"><Receipt size={16}/></button>}</div></div></div>))}</div></div>
      )}

      {selectedSale && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4 animate-in zoom-in-95"><div className="bg-white rounded-lg shadow-2xl max-w-3xl w-full overflow-hidden flex flex-col max-h-[90vh]"><div className={`text-white p-8 flex justify-between items-start ${viewDocType === 'QUOTATION' ? 'bg-purple-700' : viewDocType === 'INVOICE' ? 'bg-slate-800' : viewDocType === 'DO' ? 'bg-blue-700' : viewDocType === 'CANCELLATION' ? 'bg-red-700' : 'bg-green-700'}`}><div><h2 className="text-4xl font-bold tracking-widest uppercase">{viewDocType === 'DO' ? 'DELIVERY ORDER' : viewDocType === 'CANCELLATION' ? 'NOTICE OF CANCELLATION' : viewDocType}</h2><p className="opacity-80 text-sm mt-1">Ref: #{selectedSale.invoiceId}</p></div><div className="text-right"><h3 className="font-bold text-xl">Double Good Farming</h3><p className="opacity-80 text-sm mt-1">123 Industrial Park<br/>Kuala Lumpur</p></div></div><div className="overflow-y-auto flex-1 bg-white p-8"><div className="flex justify-between mb-8"><div><p className="text-xs font-bold text-slate-400 uppercase mb-2">Customer Details</p><h4 className="text-2xl font-bold text-slate-800">{selectedSale.customerName}</h4><p className="mt-3 text-sm text-slate-600">{selectedSale.shippingAddress}</p></div><div className="text-right"><p className="text-slate-500">Date: {new Date(selectedSale.dateCreated).toLocaleDateString()}</p></div></div><table className="w-full text-left mb-8"><thead className="bg-slate-50 text-slate-500 uppercase text-xs"><tr><th className="px-4 py-3">Item</th><th className="px-4 py-3 text-center">Qty</th><th className="px-4 py-3 text-right">Amount</th></tr></thead><tbody className="divide-y divide-slate-100">{selectedSale.items.map((item, i) => (<tr key={i}><td className="px-4 py-4"><p className="font-bold text-slate-800">{item.recipeName}</p></td><td className="px-4 py-4 text-center font-bold">{item.quantity}</td><td className="px-4 py-4 text-right font-bold">RM {(item.quantity * item.unitPrice).toFixed(2)}</td></tr>))}</tbody></table><div className="flex justify-end border-t pt-4"><div className="text-right text-2xl font-bold"><span>Total: </span><span>RM {selectedSale.totalAmount.toFixed(2)}</span></div></div></div><div className="p-6 bg-slate-50 border-t border-slate-200 flex justify-between"><button onClick={() => setSelectedSale(null)} className="px-6 py-2 bg-white border font-bold rounded">Close</button><button onClick={() => window.print()} className="px-6 py-2 bg-slate-800 text-white font-bold rounded flex items-center gap-2"><Printer size={16}/> Print</button></div></div></div>
      )}

      {showCancelModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4 animate-in fade-in"><div className="bg-white p-6 rounded-2xl shadow-xl max-w-sm w-full"><h3 className="font-bold text-lg mb-2 text-red-700 flex items-center"><XCircle className="mr-2" /> Cancel Order</h3><div className="space-y-3"><select className="w-full p-2 border rounded-lg bg-slate-50 text-sm font-medium outline-none" value={cancelReason} onChange={(e) => setCancelReason(e.target.value)}><option value="Customer changed mind">Customer changed mind</option><option value="Stock unavailable">Stock unavailable</option><option value="Pricing issue">Pricing issue</option><option value="Duplicate order">Duplicate order</option><option value="Others">Others</option></select>{cancelReason === 'Others' && <textarea className="w-full p-2 border rounded-lg text-sm" placeholder="Type reason here..." value={customCancelReason} onChange={(e) => setCustomCancelReason(e.target.value)}></textarea>}<button onClick={handleCancelSale} className="w-full py-3 bg-red-600 text-white font-bold rounded-lg hover:bg-red-700 mt-2">Confirm Cancellation</button><button onClick={() => setShowCancelModal(null)} className="w-full py-2 text-slate-500 font-bold hover:bg-slate-50 rounded-lg">Back</button></div></div></div>
      )}

      {showCustomerModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4 animate-in fade-in"><div className="bg-white p-6 rounded-2xl w-full max-w-sm shadow-xl"><div className="flex justify-between items-center mb-4"><h3 className="font-bold text-lg flex items-center text-slate-800"><User className="mr-2 text-nature-600"/> Add New Customer</h3><button onClick={() => setShowCustomerModal(false)}><X size={20}/></button></div><form onSubmit={handleAddCustomer} className="space-y-3"><input required placeholder="Name" className="w-full p-2.5 border rounded-lg bg-slate-50 text-sm" value={newCustomer.name || ''} onChange={e => setNewCustomer({...newCustomer, name: e.target.value})} /><input required placeholder="Contact No." className="w-full p-2.5 border rounded-lg bg-slate-50 text-sm" value={newCustomer.contact || ''} onChange={e => setNewCustomer({...newCustomer, contact: e.target.value})} /><button type="submit" className="w-full py-3 bg-nature-600 text-white font-bold rounded-xl mt-4 shadow-lg hover:bg-nature-700 transition-all">Save Customer</button></form></div></div>
      )}

      {showSupplierModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4 animate-in fade-in"><div className="bg-white rounded-3xl shadow-2xl max-w-sm w-full p-8 flex flex-col"><div className="flex items-center gap-3 mb-6"><LayoutList size={32} strokeWidth={2.5} /><h3 className="font-bold text-2xl text-slate-800">Manage Suppliers</h3></div><form onSubmit={handleAddSupplier} className="space-y-5"><div><p className="text-[11px] font-extrabold text-slate-500 uppercase tracking-wider mb-3">Add New Supplier & Item</p><div className="space-y-3"><input required placeholder="Supplier Name" className="w-full p-3 bg-white border border-slate-200 rounded-lg text-sm" value={newSupplier.name} onChange={e => setNewSupplier({...newSupplier, name: e.target.value})} /><input required placeholder="Contact" className="w-full p-3 bg-white border border-slate-200 rounded-lg text-sm" value={newSupplier.contact} onChange={e => setNewSupplier({...newSupplier, contact: e.target.value})} /></div></div><div className="pt-2"><p className="text-[11px] font-extrabold text-slate-500 uppercase tracking-wider mb-3">Primary Item Supplied</p><div className="space-y-3"><input required placeholder="Item Name" className="w-full p-3 bg-white border border-slate-200 rounded-lg text-sm" value={newSupplier.itemName} onChange={e => setNewSupplier({...newSupplier, itemName: e.target.value})} /><div className="grid grid-cols-2 gap-3"><input required type="number" placeholder="Pack Size" className="w-full p-3 bg-white border border-slate-200 rounded-lg text-sm" value={newSupplier.packSize} onChange={e => setNewSupplier({...newSupplier, packSize: Number(e.target.value)})} /><input required type="number" placeholder="Cost (RM)" className="w-full p-3 bg-white border border-slate-200 rounded-lg text-sm" value={newSupplier.unitCost} onChange={e => setNewSupplier({...newSupplier, unitCost: Number(e.target.value)})} /></div></div></div><button type="submit" className="w-full py-4 bg-blue-600 hover:bg-blue-700 text-white rounded-xl font-bold text-lg shadow-lg mt-4">Register Supplier & Item</button></form><button onClick={() => setShowSupplierModal(false)} className="mt-4 text-slate-500 font-bold text-center w-full">Close</button></div></div>
      )}

      {showOrderModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4 animate-in fade-in"><div className="bg-white p-6 rounded-2xl shadow-xl max-w-sm w-full"><div className="flex justify-between items-center mb-6"><h3 className="font-bold text-lg text-slate-800 flex items-center"><ShoppingCart className="mr-2 text-earth-600"/> New Purchase Order</h3><button onClick={() => setShowOrderModal(false)}><X size={20}/></button></div><form onSubmit={handleCreatePO} className="space-y-4"><div><label className="text-[10px] font-bold text-slate-500 uppercase">Packaging Item</label><select className="w-full p-2.5 border rounded-lg bg-slate-50 text-sm font-medium" required value={poItem} onChange={e => setPoItem(e.target.value)}><option value="">Select Item...</option>{purchaseableItems.map(i => (<option key={i.id} value={i.id}>{i.name} ({i.quantity} left)</option>))}</select></div><div><label className="text-[10px] font-bold text-slate-500 uppercase">Quantity (Packs)</label><input type="number" min="1" required className="w-full p-2.5 border border-slate-200 rounded-lg bg-white text-base font-medium outline-none focus:ring-2 focus:ring-blue-100" value={poQtyPackages} onChange={e => setPoQtyPackages(e.target.value)} /></div>{(() => { const item = inventory.find(i => i.id === poItem); if (!item) return null; const packSize = item.packSize || 1; const totalUnits = Number(poQtyPackages) * packSize; return (<div className="p-3 bg-blue-50/50 border border-blue-100 rounded-lg text-xs"><span className="text-slate-600 font-bold">1 Pack = {packSize} units. </span><span className="text-nature-700 font-black">Total: {totalUnits} units.</span></div>); })()}<div className="flex gap-3 pt-2"><button type="button" onClick={() => setShowOrderModal(false)} className="flex-1 py-3 bg-slate-100/50 text-slate-600 font-bold rounded-lg hover:bg-slate-100">Cancel</button><button type="submit" className="flex-1 py-3 bg-[#292524] text-white font-bold rounded-lg hover:bg-black transition-all">Place Order</button></div></form></div></div>
      )}

      {showQCModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4"><div className="bg-white p-6 rounded-2xl shadow-xl max-w-sm w-full text-center"><div className="w-16 h-16 bg-blue-50 text-blue-600 rounded-full flex items-center justify-center mx-auto mb-4"><PackageCheck size={32}/></div><h3 className="font-bold text-lg mb-2">Receive Shipment</h3><p className="text-slate-500 text-sm mb-6">Has the shipment arrived in good condition and passed quality inspection?</p><div className="flex gap-3"><button onClick={() => handleQC(false)} className="flex-1 py-3 border-2 border-red-200 text-red-600 font-bold rounded-xl hover:bg-red-50">Fail QC</button><button onClick={() => handleQC(true)} className="flex-1 py-3 bg-nature-600 text-white font-bold rounded-xl hover:bg-nature-700 shadow-lg">Pass & Receive</button></div></div></div>
      )}

      {showComplaintModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4"><div className="bg-white p-6 rounded-2xl shadow-xl max-w-sm w-full"><h3 className="font-bold text-lg mb-2 text-red-700 flex items-center"><AlertTriangle className="mr-2"/> Log QC Complaint</h3><textarea className="w-full p-3 border rounded-xl text-sm mb-4 bg-slate-50" placeholder="Describe the quality issue..." value={complaintReason} onChange={e => setComplaintReason(e.target.value)} rows={4}></textarea><button onClick={handleSubmitComplaint} className="w-full py-3 bg-red-600 text-white font-bold rounded-xl shadow-lg">Submit Complaint</button></div></div>
      )}

      {showRateModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4"><div className="bg-white p-6 rounded-2xl shadow-xl max-w-sm w-full"><div className="flex justify-between items-center mb-6"><h3 className="font-bold text-lg">System Rates</h3><button onClick={() => setShowRateModal(false)}><X size={20}/></button></div><div className="space-y-4"><div><label className="text-[10px] font-bold text-slate-500 uppercase">Raw Material Rate (RM/kg)</label><input type="number" className="w-full p-2 border rounded" value={rawRate} onChange={e => { setRawMaterialRate(parseFloat(e.target.value)); setRawMaterialRateState(parseFloat(e.target.value)); }} /></div><div><label className="text-[10px] font-bold text-slate-500 uppercase">Labor Rate (RM/hour)</label><input type="number" className="w-full p-2 border rounded" value={laborRate} onChange={e => { setLaborRate(parseFloat(e.target.value)); setLaborRateState(parseFloat(e.target.value)); }} /></div></div></div></div>
      )}
    </div>
  );
};

export default FinancePage;
