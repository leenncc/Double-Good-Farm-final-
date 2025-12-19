
import React, { useEffect, useState } from 'react';
import { fetchBatches, packRecipeFIFO, getInventory, getPackingHistory } from '../services/sheetService';
import { MushroomBatch, BatchStatus, InventoryItem, FinishedGood } from '../types';
import { Package, Box, AlertCircle, Clock, CheckCircle2, History, Scale, Layers } from 'lucide-react';

type RecipeGroup = {
  name: string;
  totalWeight: number;
  batchCount: number;
};

const PackingPage: React.FC = () => {
  const [batches, setBatches] = useState<MushroomBatch[]>([]);
  const [inventory, setInventory] = useState<InventoryItem[]>([]);
  const [history, setHistory] = useState<FinishedGood[]>([]);
  
  const [selectedRecipe, setSelectedRecipe] = useState<RecipeGroup | null>(null);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [isPacking, setIsPacking] = useState(false);
  
  const [packagingType, setPackagingType] = useState<'TIN' | 'POUCH'>('POUCH');
  const [packCount, setPackCount] = useState<string>('');
  const [weightToPack, setWeightToPack] = useState<string>('');
  
  const refreshData = async () => {
    setLoading(true);
    const [batchRes, invRes, histRes] = await Promise.all([fetchBatches(), getInventory(), getPackingHistory()]);
    if (batchRes.success && batchRes.data) {
      const ready = batchRes.data.filter(b => 
        ((b.status === BatchStatus.DRYING_COMPLETE && (b.remainingWeightKg === undefined || b.remainingWeightKg > 0.01))) || 
        (b.status === BatchStatus.PACKED && (b.remainingWeightKg || 0) > 0.1)
      );
      setBatches(ready);
    }
    if (invRes.success && invRes.data) setInventory(invRes.data);
    if (histRes.success && histRes.data) setHistory(histRes.data);
    setLoading(false);
  };

  useEffect(() => { refreshData(); }, []);

  const groupedBatches = batches.reduce((acc, batch) => {
      const recipe = batch.selectedRecipeName || batch.recipeType || 'Unknown';
      if (!acc[recipe]) { acc[recipe] = { name: recipe, totalWeight: 0, batchCount: 0 }; }
      
      // KG ADJUSTMENT: Deduct processing wastage from the weight available to pack
      const grossWeight = batch.remainingWeightKg ?? batch.netWeightKg;
      const wastage = batch.processingWastageKg || 0;
      acc[recipe].totalWeight += Math.max(0, grossWeight - wastage);
      
      acc[recipe].batchCount += 1;
      return acc;
  }, {} as Record<string, RecipeGroup>);

  const handleOpenPackModal = (recipeGroup: RecipeGroup) => {
    setSelectedRecipe(recipeGroup);
    setPackagingType('POUCH'); 
    setWeightToPack('');
    setPackCount('');
    setShowModal(true);
  };

  const recipeRatio = (selectedRecipe?.name || '').toLowerCase().includes('dried') ? 0.3 : 0.4;
  const packSize = packagingType === 'POUCH' ? 0.1 : 0.2;
  const calculatedYield = weightToPack ? Math.floor((parseFloat(weightToPack) * recipeRatio) / packSize) : 0;

  const handleCompletePacking = async () => {
    if (!selectedRecipe || !weightToPack) return;
    if (calculatedYield <= 0) { alert("Weight is too low to produce a single unit."); return; }
    setIsPacking(true);
    setTimeout(async () => {
        const finalCount = packCount ? parseInt(packCount) : calculatedYield;
        const res = await packRecipeFIFO(selectedRecipe.name, parseFloat(weightToPack), finalCount, packagingType);
        if (res.success) { setShowModal(false); setSelectedRecipe(null); refreshData(); } else { alert(res.message); }
        setIsPacking(false);
    }, 3000);
  };

  const pouchTotal = inventory
    .filter(i => i.subtype === 'POUCH' || (i.name || '').toLowerCase().includes('pouch'))
    .reduce((sum, item) => sum + item.quantity, 0);

  const tinTotal = inventory
    .filter(i => i.subtype === 'TIN' || (i.name || '').toLowerCase().includes('tin'))
    .reduce((sum, item) => sum + item.quantity, 0);

  const labelTotal = inventory
    .filter(i => i.subtype === 'STICKER' || i.type === 'LABEL')
    .reduce((sum, item) => sum + item.quantity, 0);
  
  const containerTotal = packagingType === 'POUCH' ? pouchTotal : tinTotal;
  const needed = packCount ? parseInt(packCount) : calculatedYield;
  const hasContainer = containerTotal >= needed;
  const hasLabels = labelTotal >= needed;
  const hasStock = hasContainer && hasLabels;
  const isYieldValid = calculatedYield > 0;
  
  const missingItems = [];
  if (!hasContainer) missingItems.push(`${needed - containerTotal} ${packagingType === 'POUCH' ? 'Pouches' : 'Tins'}`);
  if (!hasLabels) missingItems.push(`${needed - labelTotal} Stickers`);
  const THRESHOLD = 100; 

  return (
    <div className="space-y-6 relative">
      {isPacking && (
        <div className="fixed inset-0 z-[60] bg-black/80 backdrop-blur-sm flex flex-col items-center justify-center text-white">
            <div className="w-24 h-24 rounded-full border-4 border-nature-500 border-t-transparent animate-spin mb-6"></div>
            <h2 className="text-3xl font-bold animate-pulse">Sealing & Labeling...</h2>
        </div>
      )}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className={`p-4 rounded-xl border flex items-center justify-between ${pouchTotal < THRESHOLD ? 'bg-red-50 border-red-200' : 'bg-white border-slate-200'}`}>
              <div><p className="text-xs font-bold text-slate-500 uppercase">PACKAGING</p><p className="font-bold text-slate-800">Pouches (All Types)</p></div>
              <div className="text-right"><p className={`text-2xl font-bold ${pouchTotal < THRESHOLD ? 'text-red-600' : 'text-slate-800'}`}>{pouchTotal}</p></div>
          </div>
          <div className={`p-4 rounded-xl border flex items-center justify-between ${tinTotal < THRESHOLD ? 'bg-red-50 border-red-200' : 'bg-white border-slate-200'}`}>
              <div><p className="text-xs font-bold text-slate-500 uppercase">PACKAGING</p><p className="font-bold text-slate-800">Metal Tins</p></div>
              <div className="text-right"><p className={`text-2xl font-bold ${tinTotal < THRESHOLD ? 'text-red-600' : 'text-slate-800'}`}>{tinTotal}</p></div>
          </div>
          <div className={`p-4 rounded-xl border flex items-center justify-between ${labelTotal < THRESHOLD ? 'bg-red-50 border-red-200' : 'bg-white border-slate-200'}`}>
              <div><p className="text-xs font-bold text-slate-500 uppercase">LABEL</p><p className="font-bold text-slate-800">Stickers / Labels</p></div>
              <div className="text-right"><p className={`text-2xl font-bold ${labelTotal < THRESHOLD ? 'text-red-600' : 'text-slate-800'}`}>{labelTotal}</p></div>
          </div>
      </div>

      <div className="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden">
        <div className="p-6 border-b border-slate-100 flex justify-between items-center">
          <div><h3 className="font-semibold text-slate-800">Ready to Pack</h3></div>
          <button onClick={refreshData} className="text-sm text-nature-600 hover:underline">Refresh</button>
        </div>
        {loading ? <div className="p-8 text-center text-slate-400">Loading...</div> : (
          <table className="w-full text-left">
              <thead className="bg-slate-50 text-slate-500 text-xs uppercase">
                <tr><th className="px-6 py-4">Recipe / Product</th><th className="px-6 py-4">Avail. Batches</th><th className="px-6 py-4">Total Weight</th><th className="px-6 py-4 text-right">Action</th></tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {Object.values(groupedBatches).map((group: RecipeGroup, idx) => (
                  <tr key={idx} className="hover:bg-slate-50">
                    <td className="px-6 py-4 font-bold text-slate-800">{group.name}</td>
                    <td className="px-6 py-4 text-slate-500"><Layers size={16} className="inline mr-2 text-earth-500"/> {group.batchCount} batches</td>
                    <td className="px-6 py-4 font-bold text-nature-700">{group.totalWeight.toFixed(1)} kg</td>
                    <td className="px-6 py-4 text-right"><button onClick={() => handleOpenPackModal(group)} className="px-4 py-2 bg-earth-800 text-white text-sm rounded-lg hover:bg-earth-900 shadow-sm flex items-center ml-auto"><Box size={16} className="mr-2" /> Pack</button></td>
                  </tr>
                ))}
                {Object.keys(groupedBatches).length === 0 && (<tr><td colSpan={4} className="p-8 text-center text-slate-400">No batches ready for packing.</td></tr>)}
              </tbody>
            </table>
        )}
      </div>

      <div className="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden">
        <div className="p-6 border-b border-slate-100 flex items-center"><History size={20} className="text-slate-400 mr-2" /><h3 className="font-semibold text-slate-800">Recent Packing Activity</h3></div>
        <div className="overflow-x-auto">
           <table className="w-full text-left">
              <thead className="bg-slate-50 text-slate-500 text-xs uppercase">
                 <tr><th className="px-6 py-3">Packed Date</th><th className="px-6 py-3">Product</th><th className="px-6 py-3">Type</th><th className="px-6 py-3 text-right">Quantity</th></tr>
              </thead>
              <tbody className="divide-y divide-slate-100 text-sm">
                 {history.map(item => (
                    <tr key={item.id}>
                       <td className="px-6 py-3 text-slate-500">{new Date(item.datePacked).toLocaleString()}</td>
                       <td className="px-6 py-3 font-medium text-slate-800">{item.recipeName}</td>
                       <td className="px-6 py-3"><span className="px-2 py-1 bg-slate-100 rounded text-xs font-bold text-slate-600">{item.packagingType}</span></td>
                       <td className="px-6 py-3 text-right font-bold text-nature-700">{item.quantity} units</td>
                    </tr>
                 ))}
                 {history.length === 0 && <tr><td colSpan={4} className="px-6 py-8 text-center text-slate-400">No packing history found.</td></tr>}
              </tbody>
           </table>
        </div>
      </div>

      {showModal && selectedRecipe && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
          <div className="bg-white rounded-2xl shadow-2xl max-w-lg w-full p-6">
             <h3 className="text-xl font-bold text-slate-900 mb-6">Pack {selectedRecipe.name}</h3>
             <div className="space-y-4">
                <div>
                   <label className="block text-sm font-bold text-slate-700 mb-1">Packaging Type</label>
                   <div className="grid grid-cols-2 gap-3">
                      <button onClick={() => setPackagingType('POUCH')} className={`p-3 rounded-lg border-2 ${packagingType === 'POUCH' ? 'border-nature-500 bg-nature-50 text-nature-700 font-bold' : 'border-slate-200'}`}>Pouch</button>
                      <button onClick={() => setPackagingType('TIN')} className={`p-3 rounded-lg border-2 ${packagingType === 'TIN' ? 'border-nature-500 bg-nature-50 text-nature-700 font-bold' : 'border-slate-200'}`}>Tin</button>
                   </div>
                </div>
                <div>
                   <label className="block text-sm font-bold text-slate-700 mb-1">Weight to Pack (Max: {selectedRecipe.totalWeight.toFixed(1)} kg)</label>
                   <div className="relative">
                      <input type="number" className="w-full p-3 pl-10 border rounded-lg" value={weightToPack} onChange={e => setWeightToPack(e.target.value)} max={selectedRecipe.totalWeight} placeholder="0.0" />
                      <Scale size={18} className="absolute left-3 top-3.5 text-slate-400" />
                   </div>
                </div>
                <div className="bg-slate-50 p-4 rounded-lg flex justify-between items-center"><span className="text-sm font-bold text-slate-600">Estimated Output:</span><span className="text-2xl font-bold text-nature-700">{calculatedYield} units</span></div>
                {!hasStock && isYieldValid && (<div className="bg-red-50 text-red-600 p-3 rounded-lg text-sm font-bold flex flex-col items-start"><div><AlertCircle size={16} className="inline mr-2" /> Insufficient Inventory</div><span className="text-xs ml-6">Missing: {missingItems.join(', ')} (You can still proceed)</span></div>)}
                <div className="flex space-x-3 pt-4">
                   <button onClick={() => setShowModal(false)} className="flex-1 py-3 text-slate-500 hover:bg-slate-50 rounded-lg font-bold">Cancel</button>
                   <button onClick={handleCompletePacking} disabled={!weightToPack || !isYieldValid || parseFloat(weightToPack) > selectedRecipe.totalWeight} className="flex-1 py-3 bg-earth-800 text-white rounded-lg font-bold">Confirm Pack</button>
                </div>
             </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default PackingPage;
