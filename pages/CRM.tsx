import React, { useState, useEffect } from 'react';
import { getCustomers, getCustomerStats, updateCustomer, addCustomer } from '../services/sheetService';
import { Customer } from '../types';
import { Users, Building2, User, Phone, Mail, Star, Clock, ShoppingBag, MessageCircle, Search, Plus, Wallet, ArrowRight, Pencil, X, ArrowLeft, Calendar, FileText, Printer, ChevronRight, Tags } from 'lucide-react';

const CRMPage: React.FC = () => {
  const [customers, setCustomers] = useState<Customer[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [stats, setStats] = useState<any>(null);
  const [filterType, setFilterType] = useState<'ALL' | 'B2B' | 'B2C'>('ALL');
  const [searchTerm, setSearchTerm] = useState('');
  
  const [showAddModal, setShowAddModal] = useState(false);
  const [newCustomer, setNewCustomer] = useState<Partial<Customer>>({ type: 'B2C', status: 'ACTIVE' });
  const [showEditModal, setShowEditModal] = useState(false);
  const [editingCustomer, setEditingCustomer] = useState<Partial<Customer>>({});

  useEffect(() => { loadData(); }, []);
  useEffect(() => { if (selectedId) loadStats(selectedId); }, [selectedId]);

  const loadData = async () => {
    const res = await getCustomers();
    if (res.success && res.data) setCustomers(res.data);
  };

  const loadStats = async (id: string) => {
    const data = await getCustomerStats(id);
    setStats(data);
  };

  const handleAddSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    await addCustomer({ 
        id: `cust-${Date.now()}`, 
        name: newCustomer.name || 'Unknown', 
        email: newCustomer.email || '', 
        contact: newCustomer.contact || '', 
        address: newCustomer.address || '', 
        type: newCustomer.type as 'B2B' | 'B2C' || 'B2C', 
        status: newCustomer.status as 'ACTIVE' | 'INACTIVE' | 'VIP' || 'ACTIVE', 
        joinDate: new Date().toISOString(),
        favoriteProduct: newCustomer.favoriteProduct || ''
    } as Customer);
    setShowAddModal(false);
    setNewCustomer({ type: 'B2C', status: 'ACTIVE' });
    loadData();
  };

  const handleEditClick = () => {
      const customer = customers.find(c => c.id === selectedId);
      if (customer) { setEditingCustomer({ ...customer }); setShowEditModal(true); }
  };

  const handleUpdateSubmit = async (e: React.FormEvent) => {
      e.preventDefault();
      if (editingCustomer.id) { 
          await updateCustomer(editingCustomer.id, editingCustomer); 
          setShowEditModal(false); 
          loadData(); 
      }
  };

  const sendWhatsApp = (phone: string, name: string, type: 'PROMO' | 'UPDATE') => {
      const cleanPhone = phone?.replace(/\D/g, '') || '';
      if (cleanPhone.length < 9) { alert("Invalid phone number for WhatsApp"); return; }
      let msg = type === 'PROMO' ? `Hi ${name}! We have fresh mushrooms harvested today. Interested?` : `Hi ${name}, just checking in on your last order. Everything good?`;
      window.open(`https://wa.me/${cleanPhone}?text=${encodeURIComponent(msg)}`, '_blank');
  };

  const filteredCustomers = customers.filter(c => {
      const term = (searchTerm || '').toLowerCase();
      const matchesSearch = (c.name || '').toLowerCase().includes(term);
      const matchesType = filterType === 'ALL' || c.type === filterType;
      return matchesSearch && matchesType;
  });

  const selectedCustomer = customers.find(c => c.id === selectedId);

  return (
    <div className="flex flex-col lg:flex-row h-[calc(100vh-8rem)] lg:h-[calc(100vh-2rem)] gap-4 lg:gap-6">
      <div className={`w-full lg:w-1/3 bg-white rounded-2xl shadow-sm border border-slate-200 flex flex-col overflow-hidden ${selectedId ? 'hidden lg:flex' : 'flex'}`}>
        <div className="p-4 border-b border-slate-100 space-y-4">
            <div className="flex justify-between items-center"><h2 className="font-bold text-slate-800 flex items-center"><Users className="mr-2 text-earth-600"/> CRM</h2><button onClick={() => setShowAddModal(true)} className="p-2 bg-nature-600 text-white rounded-lg hover:bg-nature-700 transition-colors"><Plus size={20} /></button></div>
            <div className="relative"><Search className="absolute left-3 top-3 text-slate-400" size={16} /><input className="w-full pl-10 p-2.5 bg-slate-50 border border-slate-200 rounded-lg text-sm" placeholder="Search customers..." value={searchTerm} onChange={e => setSearchTerm(e.target.value)}/></div>
            <div className="flex gap-2">
                {['ALL', 'B2B', 'B2C'].map(t => (
                    <button key={t} onClick={() => setFilterType(t as any)} className={`flex-1 py-1.5 text-xs font-bold rounded-md transition-all ${filterType === t ? 'bg-earth-800 text-white shadow-md' : 'bg-slate-50 text-slate-500'}`}>{t}</button>
                ))}
            </div>
        </div>
        <div className="flex-1 overflow-y-auto p-2 space-y-2">
            {filteredCustomers.map(c => (
                <div key={c.id} onClick={() => setSelectedId(c.id)} className={`p-3 rounded-xl border cursor-pointer transition-all hover:bg-slate-50 ${selectedId === c.id ? 'bg-earth-50 border-earth-300' : 'bg-white border-transparent'}`}>
                    <div className="flex justify-between items-start">
                      <div className="min-w-0">
                        <h4 className="font-black text-slate-800 text-sm truncate">{c.name}</h4>
                        <p className="text-xs text-slate-400 truncate mt-0.5">{c.email}</p>
                      </div>
                      <div className="flex flex-col items-end gap-1">
                          <span className="text-[9px] font-black px-2 py-0.5 rounded border uppercase bg-slate-50 text-slate-500 border-slate-200">{c.type || 'B2C'}</span>
                          {c.status === 'VIP' && <span className="text-[8px] font-black px-1.5 bg-amber-100 text-amber-600 rounded">VIP</span>}
                      </div>
                    </div>
                </div>
            ))}
        </div>
      </div>

      <div className={`flex-1 bg-white rounded-3xl shadow-sm border border-slate-200 overflow-hidden flex flex-col ${selectedId ? 'flex' : 'hidden lg:flex'}`}>
          {selectedCustomer ? (
              <div className="flex flex-col h-full bg-white">
                  {/* --- REDESIGNED HEADER --- */}
                  <div className="bg-[#292524] p-8 text-white relative">
                      <button onClick={() => setSelectedId(null)} className="absolute top-4 left-4 p-2 bg-white/10 hover:bg-white/20 text-white rounded-full lg:hidden backdrop-blur-sm z-10 transition-all"><ArrowLeft size={20} /></button>
                      
                      <div className="flex justify-between items-start">
                          <div className="space-y-4">
                              <h1 className="text-4xl font-black tracking-tight">{selectedCustomer.name}</h1>
                              <div className="flex flex-col space-y-2">
                                  <div className="flex items-center gap-2 text-sm text-slate-300 font-bold">
                                      <Mail size={16} /> {selectedCustomer.email}
                                  </div>
                                  <div className="flex items-center gap-2 text-sm text-slate-300 font-bold">
                                      <Phone size={16} /> {selectedCustomer.contact}
                                  </div>
                              </div>
                          </div>

                          <div className="text-right flex flex-col items-end gap-3">
                              <button onClick={handleEditClick} className="p-2 bg-white/10 hover:bg-white/20 rounded-lg text-white backdrop-blur-sm transition-all border border-white/10">
                                  <Pencil size={20} />
                              </button>
                              <div className="flex flex-col items-end">
                                  <span className="text-[10px] text-slate-400 font-black uppercase tracking-widest mb-1">Status & Type</span>
                                  <div className="flex items-center gap-2">
                                      <div className={`w-3 h-3 rounded-full ${selectedCustomer.status === 'ACTIVE' ? 'bg-green-500' : selectedCustomer.status === 'VIP' ? 'bg-amber-500' : 'bg-slate-500'}`}></div>
                                      <span className="text-2xl font-black">{selectedCustomer.status || 'ACTIVE'}</span>
                                      <span className="text-slate-500 mx-1">/</span>
                                      <span className="text-lg font-bold text-slate-400">{selectedCustomer.type === 'B2B' ? 'B2B' : 'Retail'}</span>
                                  </div>
                              </div>
                          </div>
                      </div>
                  </div>

                  {/* --- KPI TILES --- */}
                  <div className="p-8 grid grid-cols-2 md:grid-cols-4 gap-4">
                      <div className="bg-slate-50/80 p-5 rounded-2xl border border-slate-100 flex flex-col gap-2 transition-all hover:shadow-sm">
                          <div className="flex items-center gap-2 text-slate-400">
                              <Wallet size={14} className="opacity-60" />
                              <span className="text-[10px] font-black uppercase tracking-widest">Total Spent</span>
                          </div>
                          <p className="text-2xl font-black text-slate-800">RM {stats?.totalSpent?.toFixed(2) || '0.00'}</p>
                      </div>
                      <div className="bg-slate-50/80 p-5 rounded-2xl border border-slate-100 flex flex-col gap-2 transition-all hover:shadow-sm">
                          <div className="flex items-center gap-2 text-slate-400">
                              <ShoppingBag size={14} className="opacity-60" />
                              <span className="text-[10px] font-black uppercase tracking-widest">Orders</span>
                          </div>
                          <p className="text-2xl font-black text-slate-800">{stats?.orderCount || 0}</p>
                      </div>
                      <div className="bg-slate-50/80 p-5 rounded-2xl border border-slate-100 flex flex-col gap-2 transition-all hover:shadow-sm">
                          <div className="flex items-center gap-2 text-slate-400">
                              <Star size={14} className="opacity-60" />
                              <span className="text-[10px] font-black uppercase tracking-widest">Favorite</span>
                          </div>
                          <p className="text-lg font-black text-slate-800 truncate" title={selectedCustomer.favoriteProduct || stats?.favoriteProduct || 'None'}>
                              {selectedCustomer.favoriteProduct || stats?.favoriteProduct || 'None'}
                          </p>
                      </div>
                      <div className="bg-slate-50/80 p-5 rounded-2xl border border-slate-100 flex flex-col gap-2 transition-all hover:shadow-sm">
                          <div className="flex items-center gap-2 text-slate-400">
                              <Clock size={14} className="opacity-60" />
                              <span className="text-[10px] font-black uppercase tracking-widest">Last Order</span>
                          </div>
                          <p className="text-lg font-black text-slate-800">
                              {stats?.lastOrderDate && stats.lastOrderDate !== 'Never' ? new Date(stats.lastOrderDate).toLocaleDateString() : 'Never'}
                          </p>
                      </div>
                  </div>

                  {/* --- SCROLLABLE CONTENT --- */}
                  <div className="flex-1 overflow-y-auto px-8 pb-8 space-y-8 custom-scrollbar">
                      {/* ENGAGEMENT ACTIONS */}
                      <div className="space-y-4 pt-4 border-t border-slate-100">
                          <h3 className="font-black text-slate-800 text-sm uppercase tracking-tight">Engagement Actions</h3>
                          <div className="flex flex-col md:flex-row gap-4">
                              <button 
                                onClick={() => sendWhatsApp(selectedCustomer.contact || '', selectedCustomer.name, 'PROMO')} 
                                className="flex-1 py-5 bg-[#16a34a] hover:bg-[#15803d] text-white rounded-2xl font-black shadow-lg shadow-green-100 flex items-center justify-center gap-3 transition-all transform active:scale-95"
                              >
                                  <MessageCircle size={24} /> Send Stock Update (WhatsApp)
                              </button>
                              <button 
                                onClick={() => sendWhatsApp(selectedCustomer.contact || '', selectedCustomer.name, 'UPDATE')} 
                                className="flex-1 py-5 bg-white text-slate-700 border-2 border-slate-200 hover:bg-slate-50 rounded-2xl font-black flex items-center justify-center gap-3 transition-all transform active:scale-95"
                              >
                                  <MessageCircle size={24} className="text-slate-400" /> Follow Up Message
                              </button>
                          </div>
                      </div>

                      {/* CUSTOMER NOTES */}
                      <div className="space-y-3">
                          <h3 className="font-black text-slate-800 text-sm uppercase tracking-tight">Customer Notes</h3>
                          <div className="relative group">
                              <textarea 
                                className="w-full h-32 p-5 bg-white border border-slate-200 rounded-2xl text-sm font-medium text-slate-600 focus:ring-2 focus:ring-nature-500 outline-none transition-all placeholder:text-slate-300 resize-none shadow-sm" 
                                placeholder="Add notes about preferences, delivery instructions, etc..." 
                                defaultValue={selectedCustomer.notes} 
                                onBlur={(e) => updateCustomer(selectedCustomer.id, { notes: e.target.value })}
                              ></textarea>
                              <div className="absolute bottom-3 right-3 text-slate-200 group-focus-within:text-nature-400 transition-colors">
                                  <ChevronRight size={16} className="rotate-45" />
                              </div>
                          </div>
                      </div>

                      {/* PURCHASE HISTORY TABLE */}
                      <div className="space-y-4">
                          <div className="flex items-center gap-2">
                              <ShoppingBag size={18} className="text-slate-400" />
                              <h3 className="font-black text-slate-800 text-sm uppercase tracking-tight">Purchase History</h3>
                          </div>
                          <div className="bg-white rounded-2xl border border-slate-100 shadow-sm overflow-hidden">
                              <table className="w-full text-left">
                                  <thead className="bg-slate-50/50 border-b border-slate-100">
                                      <tr className="text-[10px] font-black text-slate-400 uppercase tracking-widest">
                                          <th className="px-6 py-4">Date</th>
                                          <th className="px-6 py-4">Invoice</th>
                                          <th className="px-6 py-4">Items Purchased</th>
                                          <th className="px-6 py-4">Total</th>
                                          <th className="px-6 py-4 text-center">Status</th>
                                      </tr>
                                  </thead>
                                  <tbody className="divide-y divide-slate-50">
                                      {!stats || stats?.salesHistory?.length === 0 ? (
                                          <tr><td colSpan={5} className="p-8 text-center text-slate-300 font-bold italic">No history found.</td></tr>
                                      ) : (
                                          stats.salesHistory.map((sale: any) => (
                                              <tr key={sale.id} className="hover:bg-slate-50/50 transition-colors">
                                                  <td className="px-6 py-4 text-sm font-bold text-slate-500">
                                                      {new Date(sale.dateCreated).toLocaleDateString()}
                                                  </td>
                                                  <td className="px-6 py-4 text-xs font-mono font-bold text-slate-400 uppercase">
                                                      #{sale.invoiceId || sale.id.slice(-6)}
                                                  </td>
                                                  <td className="px-6 py-4 text-xs text-slate-600 font-medium">
                                                      <div className="max-w-[200px] truncate">
                                                        {sale.items?.map((i: any) => `${i.quantity}x ${i.recipeName}`).join(', ') || 'No details'}
                                                      </div>
                                                  </td>
                                                  <td className="px-6 py-4 text-sm font-black text-slate-800">
                                                      RM {sale.totalAmount.toFixed(2)}
                                                  </td>
                                                  <td className="px-6 py-4 text-center">
                                                      <span className={`text-[9px] font-black px-2 py-0.5 rounded uppercase tracking-widest border ${
                                                          sale.status === 'PAID' ? 'bg-green-50 text-green-600 border-green-100' :
                                                          sale.status === 'INVOICED' ? 'bg-orange-50 text-orange-600 border-orange-100' :
                                                          'bg-slate-50 text-slate-400 border-slate-200'
                                                      }`}>
                                                          {sale.status}
                                                      </span>
                                                  </td>
                                              </tr>
                                          ))
                                      )}
                                  </tbody>
                              </table>
                          </div>
                      </div>
                  </div>
              </div>
          ) : (
              <div className="flex-1 flex flex-col items-center justify-center text-slate-400 p-8 text-center"><Users size={64} className="mb-4 opacity-10" /><p className="text-lg font-black tracking-tight text-slate-300">Select a customer to view details</p></div>
          )}
      </div>

      {showAddModal && (
          <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 p-4 animate-in fade-in">
              <div className="bg-white p-8 rounded-3xl w-full max-w-sm shadow-2xl">
                  <h3 className="font-black text-xl mb-6 flex items-center gap-2">
                    <User className="text-nature-600" /> New Customer Profile
                  </h3>
                  <form onSubmit={handleAddSubmit} className="space-y-4">
                      <div className="space-y-1">
                          <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Full Name</label>
                          <input required className="w-full p-3 bg-slate-50 border border-slate-100 rounded-xl outline-none focus:ring-2 focus:ring-nature-500 transition-all" value={newCustomer.name || ''} onChange={e => setNewCustomer({...newCustomer, name: e.target.value})} />
                      </div>
                      <div className="grid grid-cols-2 gap-3">
                          <div className="space-y-1">
                              <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Email</label>
                              <input required className="w-full p-3 bg-slate-50 border border-slate-100 rounded-xl outline-none focus:ring-2 focus:ring-nature-500 transition-all" value={newCustomer.email || ''} onChange={e => setNewCustomer({...newCustomer, email: e.target.value})} />
                          </div>
                          <div className="space-y-1">
                              <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Phone</label>
                              <input required className="w-full p-3 bg-slate-50 border border-slate-100 rounded-xl outline-none focus:ring-2 focus:ring-nature-500 transition-all" value={newCustomer.contact || ''} onChange={e => setNewCustomer({...newCustomer, contact: e.target.value})} />
                          </div>
                      </div>
                      <button className="w-full py-4 bg-earth-800 text-white font-black rounded-xl mt-4 shadow-xl hover:bg-black transition-all">Create Profile</button>
                  </form>
                  <button onClick={() => setShowAddModal(false)} className="w-full mt-4 text-sm font-bold text-slate-400 hover:text-slate-600">Dismiss</button>
              </div>
          </div>
      )}

      {showEditModal && editingCustomer && (
          <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 p-4 animate-in fade-in">
              <div className="bg-white p-8 rounded-3xl w-full max-w-md shadow-2xl overflow-y-auto max-h-[90vh]">
                  <div className="flex justify-between items-center mb-6"><h3 className="font-black text-xl">Edit Profile</h3><button onClick={() => setShowEditModal(false)}><X size={24} className="text-slate-400"/></button></div>
                  <form onSubmit={handleUpdateSubmit} className="space-y-5">
                      <div className="space-y-1">
                          <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Customer Name</label>
                          <input required className="w-full p-3 bg-slate-50 border border-slate-100 rounded-xl outline-none focus:ring-2 focus:ring-nature-500" value={editingCustomer.name || ''} onChange={e => setEditingCustomer({...editingCustomer, name: e.target.value})} />
                      </div>
                      <div className="grid grid-cols-2 gap-4">
                          <div className="space-y-1">
                              <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Email Address</label>
                              <input type="email" className="w-full p-3 bg-slate-50 border border-slate-100 rounded-xl outline-none focus:ring-2 focus:ring-nature-500" value={editingCustomer.email || ''} onChange={e => setEditingCustomer({...editingCustomer, email: e.target.value})} />
                          </div>
                          <div className="space-y-1">
                              <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Phone Number</label>
                              <input type="text" className="w-full p-3 bg-slate-50 border border-slate-100 rounded-xl outline-none focus:ring-2 focus:ring-nature-500" value={editingCustomer.contact || ''} onChange={e => setEditingCustomer({...editingCustomer, contact: e.target.value})} />
                          </div>
                      </div>
                      <div className="space-y-1">
                          <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Delivery Address</label>
                          <textarea rows={2} className="w-full p-3 bg-slate-50 border border-slate-100 rounded-xl outline-none focus:ring-2 focus:ring-nature-500 resize-none" value={editingCustomer.address || ''} onChange={e => setEditingCustomer({...editingCustomer, address: e.target.value})} />
                      </div>
                      <div className="grid grid-cols-2 gap-4">
                        <div className="space-y-1">
                            <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Client Type</label>
                            <select className="w-full p-3 bg-slate-50 border border-slate-100 rounded-xl outline-none" value={editingCustomer.type} onChange={e => setEditingCustomer({...editingCustomer, type: e.target.value as any})}>
                                <option value="B2C">Individual (B2C)</option>
                                <option value="B2B">Business Partner (B2B)</option>
                            </select>
                        </div>
                        <div className="space-y-1">
                            <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Account Status</label>
                            <select className="w-full p-3 bg-slate-50 border border-slate-100 rounded-xl outline-none" value={editingCustomer.status} onChange={e => setEditingCustomer({...editingCustomer, status: e.target.value as any})}>
                                <option value="ACTIVE">ACTIVE</option>
                                <option value="INACTIVE">INACTIVE</option>
                                <option value="VIP">VIP</option>
                            </select>
                        </div>
                      </div>
                      <div className="space-y-1">
                          <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest flex items-center gap-1"><Tags size={10}/> Favorite Product (Manual)</label>
                          <input className="w-full p-3 bg-slate-50 border border-slate-100 rounded-xl outline-none focus:ring-2 focus:ring-nature-500" placeholder="e.g. Grey Oyster Chips" value={editingCustomer.favoriteProduct || ''} onChange={e => setEditingCustomer({...editingCustomer, favoriteProduct: e.target.value})} />
                          <p className="text-[9px] text-slate-400 italic">Overrides auto-calculated favorite in profile view.</p>
                      </div>
                      <button className="w-full py-4 bg-nature-600 text-white font-black rounded-xl mt-2 shadow-xl hover:bg-nature-700 transition-all">Update Database</button>
                  </form>
              </div>
          </div>
      )}
    </div>
  );
};

export default CRMPage;