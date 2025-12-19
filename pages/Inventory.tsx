import React, { useEffect, useState } from 'react';
import { getFinishedGoods, getInventory, updateFinishedGoodImage, updateFinishedGoodPrice, addInventoryItem, getSuppliers, deleteInventoryItem } from '../services/sheetService';
import { FinishedGood, InventoryItem, Supplier } from '../types';
import { Package, Box, LayoutGrid, Upload, Camera, ImageIcon, Plus, Pencil, DollarSign, Calculator, AlertTriangle, Trash2 } from 'lucide-react';

interface AggregatedGood {
  recipeName: string;
  packagingType: string;
  quantity: number;
  count: number;
  imageUrl?: string;
  sellingPrice: number;
}

const InventoryPage: React.FC = () => {
  const [inventory, setInventory] = useState<InventoryItem[]>([]);
  const [finishedGoods, setFinishedGoods] = useState<FinishedGood[]>([]);
  const [suppliers, setSuppliers] = useState<Supplier[]>([]);
  const [loading, setLoading] = useState(true);
  
  // Image Upload State
  const [showImageModal, setShowImageModal] = useState(false);
  const [selectedProductForImage, setSelectedProductForImage] = useState<{recipeName: string, packagingType: string} | null>(null);

  // Price Edit State
  const [showPriceModal, setShowPriceModal] = useState(false);
  const [editingPrice, setEditingPrice] = useState<{recipeName: string, packagingType: string, currentPrice: number} | null>(null);
  const [newPrice, setNewPrice] = useState<string>('');

  // Add Item State
  const [showAddItemModal, setShowAddItemModal] = useState(false);
  const [newItem, setNewItem] = useState<Partial<InventoryItem>>({ type: 'PACKAGING' });

  const refreshData = async () => {
    setLoading(true);
    const [invRes, fgRes, supRes] = await Promise.all([getInventory(), getFinishedGoods(), getSuppliers()]);
    
    if (invRes.success && invRes.data) setInventory(invRes.data);
    if (fgRes.success && fgRes.data) setFinishedGoods(fgRes.data.filter(g => g.quantity > 0));
    if (supRes.success && supRes.data) setSuppliers(supRes.data);
    
    setLoading(false);
  };

  useEffect(() => {
    refreshData();
  }, []);

  const handleAddItem = async (e: React.FormEvent) => {
      e.preventDefault();
      if (!newItem.name || !newItem.unitCost) return;
      
      await addInventoryItem({
          id: `inv-${Date.now()}`,
          name: newItem.name,
          type: newItem.type as any,
          subtype: newItem.subtype,
          quantity: newItem.quantity || 0,
          threshold: newItem.threshold || 10,
          unit: newItem.unit || 'units',
          unitCost: newItem.unitCost,
          supplier: newItem.supplier || 'Unknown',
          packSize: newItem.packSize || 1
      });
      
      setShowAddItemModal(false);
      setNewItem({ type: 'PACKAGING' }); 
      await refreshData();
      alert("Item added!");
  };

  const handleDeleteItem = async (id: string) => {
    if (window.confirm("Are you sure you want to delete this inventory item?")) {
        const res = await deleteInventoryItem(id);
        if (res.success) {
            refreshData();
        }
    }
  };

  const handleImageUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
      if (!selectedProductForImage || !e.target.files?.[0]) return;
      const file = e.target.files[0];
      const result = await updateFinishedGoodImage(selectedProductForImage.recipeName, selectedProductForImage.packagingType, file);
      if (result.success) {
          setShowImageModal(false);
          refreshData();
      } else {
          alert(`Error: ${result.message}`);
      }
  };

  const handleUpdatePrice = async () => {
      if (!editingPrice || !newPrice) return;
      await updateFinishedGoodPrice(editingPrice.recipeName, editingPrice.packagingType, parseFloat(newPrice));
      setShowPriceModal(false);
      refreshData();
  };

  // UPDATED AGGREGATION LOGIC
  const aggregatedGoods = finishedGoods.reduce((acc, curr) => {
     const key = `${curr.recipeName}|${curr.packagingType}`;
     if (!acc[key]) {
        acc[key] = { 
            recipeName: curr.recipeName, 
            packagingType: curr.packagingType, 
            quantity: 0, 
            count: 0,
            imageUrl: curr.imageUrl,
            sellingPrice: curr.sellingPrice !== undefined ? curr.sellingPrice : 15.00 
        };
     }
     acc[key].quantity += curr.quantity;
     acc[key].count += 1;
     
     if (curr.imageUrl && !acc[key].imageUrl) acc[key].imageUrl = curr.imageUrl;
     
     return acc;
  }, {} as Record<string, AggregatedGood>);

  const getEstimatedUnitCost = (type: string, pkg: string) => {
      const rawCost = type.includes('Chips') ? (0.1 * 4 * 8) : (0.1 * 8); 
      const pkgCost = pkg === 'TIN' ? 1.50 : 0.60;
      return rawCost + pkgCost;
  };

  const totalFinishedPacks = finishedGoods.reduce((acc, b) => acc + b.quantity, 0);

  return (
    <div className="space-y-8">
      <div>
        <h2 className="text-2xl font-bold text-slate-900 flex items-center">
          <Package className="mr-3 text-earth-600" size={32} />
          Inventory Dashboard
        </h2>
        <p className="text-slate-500">Manage packing supplies (Inputs) and finished goods (Outputs).</p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <div className="space-y-6">
          <div className="bg-white p-5 rounded-2xl shadow-sm border border-slate-200">
             <div className="flex justify-between items-center mb-4">
                <h3 className="text-lg font-bold text-slate-800 flex items-center">
                    <Box size={20} className="mr-2 text-blue-600" /> Packing Supplies
                </h3>
                <button onClick={() => setShowAddItemModal(true)} className="p-2 bg-blue-50 text-blue-600 rounded-lg hover:bg-blue-100 flex items-center text-xs font-bold">
                    <Plus size={14} className="mr-1"/> Add Item
                </button>
             </div>
             <div className="space-y-4">
               {/* UPDATED: Added .filter() to exclude FINISHED_GOOD from this list */}
               {loading ? <p className="text-slate-400">Loading stock...</p> : inventory
                 .filter(item => item.type !== 'FINISHED_GOOD') 
                 .map(item => {
                   const isLow = item.quantity < item.threshold;
                   return (
                   <div key={item.id} className={`relative p-3 rounded-xl border ${isLow ? 'bg-red-50 border-red-200' : 'bg-white border-transparent'} group`}>
                     <div className="flex mb-2 items-center justify-between">
                       <div>
                         <span className="text-xs font-semibold inline-block py-1 px-2 uppercase rounded-full bg-blue-50 text-blue-600">
                           {item.subtype || item.type}
                         </span>
                       </div>
                       <div className="flex items-center space-x-2">
                         <div className="text-right">
                           <span className={`text-sm font-bold inline-block ${isLow ? 'text-red-600' : 'text-blue-600'}`}>
                             {item.quantity} {item.unit}
                           </span>
                         </div>
                         <button onClick={() => handleDeleteItem(item.id)} className="p-1 text-slate-400 hover:text-red-500 hover:bg-red-50 rounded transition-colors"><Trash2 size={16} /></button>
                       </div>
                     </div>
                     <div className="flex mb-2 items-center justify-between text-sm"><span className="font-bold text-slate-700">{item.name}</span></div>
                     <div className="overflow-hidden h-2 mb-2 text-xs flex rounded bg-blue-100"><div style={{ width: `${Math.min((item.quantity / (item.threshold * 2)) * 100, 100)}%` }} className={`shadow-none flex flex-col text-center whitespace-nowrap text-white justify-center ${isLow ? 'bg-red-500' : 'bg-blue-500'}`}></div></div>
                     {isLow && <div className="flex items-center text-xs text-red-600 font-bold mt-1"><AlertTriangle size={12} className="mr-1" />Restock Needed</div>}
                   </div>
                 )})}
             </div>
          </div>
        </div>

        <div className="space-y-6">
          <div className="bg-white p-5 rounded-2xl shadow-sm border border-slate-200">
             <div className="flex justify-between items-center mb-4">
                <h3 className="text-lg font-bold text-slate-800 flex items-center"><LayoutGrid size={20} className="mr-2 text-green-600" /> Finished Goods</h3>
                <span className="bg-green-100 text-green-700 px-3 py-1 rounded-full text-xs font-bold">Total: {totalFinishedPacks} Packs</span>
             </div>
             
             {loading ? <p className="text-slate-400">Loading stock...</p> : Object.keys(aggregatedGoods).length === 0 ? (
               <div className="text-center py-8 bg-slate-50 rounded-lg border border-dashed border-slate-300"><p className="text-slate-400">No finished stock available.</p></div>
             ) : (
               <div className="grid grid-cols-2 gap-4">
                 {Object.values(aggregatedGoods).map((item: AggregatedGood, idx) => {
                     const estCost = getEstimatedUnitCost(item.recipeName, item.packagingType);
                     const margin = ((item.sellingPrice - estCost) / item.sellingPrice) * 100;
                     return (
                   <div key={idx} className="bg-slate-50 rounded-xl border border-slate-200 overflow-hidden group hover:shadow-md transition-all relative">
                      <div className="h-48 bg-white relative border-b border-slate-100">
                          {item.imageUrl ? <img src={item.imageUrl} alt={item.recipeName} className="w-full h-full object-contain p-2"/> : <div className="w-full h-full flex items-center justify-center text-slate-300"><ImageIcon size={32}/></div>}
                          <div className="absolute top-2 right-2">
                              <button onClick={() => { setSelectedProductForImage({ recipeName: item.recipeName, packagingType: item.packagingType }); setShowImageModal(true); }} className="p-1.5 bg-white/80 hover:bg-white rounded-full shadow-sm text-slate-600"><Camera size={16}/></button>
                          </div>
                          <span className="absolute bottom-2 left-2 bg-black/60 text-white text-[10px] px-2 py-1 rounded font-bold uppercase">{item.packagingType}</span>
                      </div>
                      <div className="p-3">
                         <p className="font-bold text-slate-800 text-sm truncate">{item.recipeName}</p>
                         <div className="mt-2 flex justify-between items-end border-b border-slate-200 pb-2 mb-2">
                             <div className="text-xs text-slate-500">Stock</div>
                             <span className="text-xl font-bold text-green-700">{item.quantity}</span>
                         </div>
                         <div className="flex justify-between items-center">
                             <div>
                                 <div className="text-[10px] text-slate-400 uppercase">Selling Price</div>
                                 <div className="font-bold text-slate-800 flex items-center">
                                     RM {item.sellingPrice.toFixed(2)}
                                     <button onClick={() => { setEditingPrice({ recipeName: item.recipeName, packagingType: item.packagingType, currentPrice: item.sellingPrice }); setNewPrice(item.sellingPrice.toString()); setShowPriceModal(true); }} className="ml-1 text-blue-500 hover:text-blue-700"><Pencil size={12}/></button>
                                 </div>
                             </div>
                             <div className="text-right">
                                 <div className="text-[10px] text-slate-400 uppercase">Est. Margin</div>
                                 <div className={`font-bold text-xs ${margin > 30 ? 'text-green-600' : 'text-orange-500'}`}>{margin.toFixed(0)}%</div>
                             </div>
                         </div>
                      </div>
                   </div>
                 )})}
               </div>
             )}
          </div>
        </div>
      </div>

      {showImageModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4">
              <div className="bg-white rounded-xl shadow-xl max-w-sm w-full p-6">
                  <h3 className="text-lg font-bold mb-4">Update Product Image</h3>
                  <label className="flex flex-col items-center justify-center w-full p-6 border-2 border-dashed border-slate-300 rounded-lg cursor-pointer hover:border-blue-500 hover:bg-blue-50 transition-colors">
                      <Upload size={32} className="text-slate-400 mb-2"/>
                      <span className="text-sm font-bold text-slate-600">Click to Upload</span>
                      <input type="file" className="hidden" accept="image/*" onChange={handleImageUpload} />
                  </label>
                  <button onClick={() => setShowImageModal(false)} className="mt-4 w-full py-2 text-slate-500 font-bold hover:bg-slate-100 rounded-lg">Cancel</button>
              </div>
          </div>
      )}

      {showPriceModal && editingPrice && (
           <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4">
              <div className="bg-white rounded-xl shadow-xl max-w-sm w-full p-6">
                  <h3 className="text-lg font-bold mb-4">Edit Selling Price</h3>
                  <p className="text-sm text-slate-500 mb-4">{editingPrice.recipeName} ({editingPrice.packagingType})</p>
                  <label className="block text-xs font-bold text-slate-500 mb-1">New Price (RM)</label>
                  <input type="number" step="0.01" className="w-full p-3 border rounded-lg mb-4 text-lg font-bold" value={newPrice} onChange={e => setNewPrice(e.target.value)} />
                  <div className="flex space-x-3">
                      <button onClick={() => setShowPriceModal(false)} className="flex-1 py-2 text-slate-500 font-bold hover:bg-slate-100 rounded-lg">Cancel</button>
                      <button onClick={handleUpdatePrice} className="flex-1 py-2 bg-green-600 text-white font-bold rounded-lg hover:bg-green-700">Update Price</button>
                  </div>
              </div>
          </div>
      )}

      {showAddItemModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4">
              <div className="bg-white rounded-xl shadow-xl max-w-md w-full p-6">
                  <h3 className="text-lg font-bold mb-4">Add Packing Supply</h3>
                  <form onSubmit={handleAddItem} className="space-y-4">
                      <div><label className="block text-xs font-bold text-slate-500 mb-1">Item Name</label><input className="w-full p-2 border rounded" required value={newItem.name || ''} onChange={e => setNewItem({...newItem, name: e.target.value})} placeholder="e.g. New Red Labels"/></div>
                      <div className="grid grid-cols-2 gap-3">
                          <div><label className="block text-xs font-bold text-slate-500 mb-1">Type</label><select className="w-full p-2 border rounded" value={newItem.type} onChange={e => setNewItem({...newItem, type: e.target.value as any})}><option value="PACKAGING">Packaging</option><option value="LABEL">Label</option><option value="OTHER">Other</option></select></div>
                          <div><label className="block text-xs font-bold text-slate-500 mb-1">Sub-Type</label><select className="w-full p-2 border rounded" value={newItem.subtype || 'POUCH'} onChange={e => setNewItem({...newItem, subtype: e.target.value as any})}><option value="POUCH">Pouch</option><option value="TIN">Tin</option><option value="STICKER">Sticker</option><option value="">None</option></select></div>
                      </div>
                      <div className="grid grid-cols-2 gap-3">
                           <div><label className="block text-xs font-bold text-slate-500 mb-1">Quantity</label><input type="number" className="w-full p-2 border rounded" value={newItem.quantity} onChange={e => setNewItem({...newItem, quantity: parseInt(e.target.value)})} /></div>
                           <div><label className="block text-xs font-bold text-slate-500 mb-1">Unit</label><input className="w-full p-2 border rounded" value={newItem.unit || 'units'} onChange={e => setNewItem({...newItem, unit: e.target.value})} /></div>
                      </div>
                      <div className="grid grid-cols-2 gap-3">
                           <div><label className="block text-xs font-bold text-slate-500 mb-1">Unit Cost</label><input type="number" placeholder="45.00" className="w-full p-2 border rounded" required value={newItem.unitCost} onChange={e => setNewItem({...newItem, unitCost: parseFloat(e.target.value)})} /></div>
                           <div><label className="block text-xs font-bold text-slate-500 mb-1">Supplier</label><select className="w-full p-2 border rounded" value={newItem.supplier || ''} onChange={e => setNewItem({...newItem, supplier: e.target.value})}><option value="">Select Supplier...</option>{suppliers.map(s => <option key={s.id} value={s.name}>{s.name}</option>)}</select></div>
                           <div><label className="block text-xs font-bold text-slate-500 mb-1">Pack Size (Units)</label><input type="number" placeholder="100" className="w-full p-2 border rounded" value={newItem.packSize} onChange={e => setNewItem({...newItem, packSize: parseFloat(e.target.value)})} /></div>
                      </div>
                      <button type="submit" className="w-full py-3 bg-blue-600 text-white font-bold rounded-lg hover:bg-blue-700">Add Item</button>
                  </form>
                  <button onClick={() => setShowAddItemModal(false)} className="mt-3 w-full text-sm text-slate-500">Cancel</button>
              </div>
          </div>
      )}
    </div>
  );
};

export default InventoryPage;