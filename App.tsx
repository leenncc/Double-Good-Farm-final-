
import React, { useState, useEffect } from 'react';
import { auth } from './services/firebase';
import { getUserRole } from './services/sheetService';
import Login from './components/Login';
import ProcessingPage from './pages/Processing';
import InventoryPage from './pages/Inventory';
import FinancePage from './pages/Finance';
import ReceivingPage from './pages/Receiving';
import PackingPage from './pages/Packing';
import CRMPage from './pages/CRM';
import ShopPage from './pages/Shop';
import { Utensils, Package, ShoppingCart, DollarSign, LayoutDashboard, LogOut, LayoutGrid, Box, Users, Sprout, Menu, X } from 'lucide-react';

export default function App() {
  // 1. CHECK URL FIRST FOR PUBLIC SHOP ACCESS
  const isShop = window.location.pathname === '/shop';

  // 2. IF SHOP, RENDER SHOP (Bypass authentication check)
  if (isShop) {
     return <ShopPage />;
  }

  const [user, setUser] = useState<any>(null);
  const [role, setRole] = useState<string>('LOADING');
  const [currentView, setCurrentView] = useState<string>('DASHBOARD');
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);

  useEffect(() => {
    const unsubscribe = auth.onAuthStateChanged(async (u) => {
      setUser(u);
      if (u) {
        const r = await getUserRole();
        setRole(r);
        // Set default view based on role
        if (r === 'PROCESSING') setCurrentView('PROCESSING');
        else if (r === 'PACKING') setCurrentView('PACKING');
        else if (r === 'SALES') setCurrentView('SALES');
        else setCurrentView('DASHBOARD');
      } else {
        setRole('GUEST');
      }
    });
    return () => unsubscribe();
  }, []);

  const handleViewChange = (view: string) => {
    setCurrentView(view);
    setIsSidebarOpen(false);
  };

  if (!user) return <Login onLogin={() => {}} />;
  if (role === 'LOADING') return <div className="p-10 text-center flex items-center justify-center h-screen text-slate-400">Loading Access...</div>;

  if (role === 'GUEST') {
    return (
      <div className="h-screen flex flex-col items-center justify-center p-6 text-center bg-slate-50">
        <div className="bg-white p-8 rounded-xl shadow-lg max-w-md w-full">
            <h1 className="text-2xl font-bold mb-2 text-slate-800">Access Pending</h1>
            <p className="text-slate-500 mb-4">Your ID: <code className="bg-slate-100 p-1 rounded text-sm font-mono">{user.uid}</code></p>
            <p className="mb-6 text-slate-600">Please ask an Admin to assign you the <b>PROCESSING</b>, <b>PACKING</b>, <b>SALES</b>, or <b>ADMIN</b> role in the database.</p>
            <button onClick={() => auth.signOut()} className="text-red-500 hover:text-red-700 font-bold underline">Sign Out</button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-screen bg-slate-50 overflow-hidden">
      
      {/* MOBILE OVERLAY */}
      {isSidebarOpen && (
        <div 
            className="fixed inset-0 bg-black/50 z-20 lg:hidden"
            onClick={() => setIsSidebarOpen(false)}
        />
      )}

      {/* SIDEBAR - Responsive */}
      <nav className={`
            fixed inset-y-0 left-0 z-30 w-64 bg-slate-900 border-r border-slate-800 p-4 flex flex-col gap-2 shadow-2xl text-white
            transition-transform duration-300 ease-in-out
            lg:relative lg:translate-x-0
            ${isSidebarOpen ? 'translate-x-0' : '-translate-x-full'}
      `}>
        {/* Mobile Header in Sidebar */}
        <div className="lg:hidden flex justify-between items-center mb-4 px-2">
            <span className="font-bold text-lg">Menu</span>
            <button onClick={() => setIsSidebarOpen(false)} className="text-slate-400 hover:text-white">
                <X size={24} />
            </button>
        </div>
        
        {/* Brand - Desktop */}
        <div className="hidden lg:flex items-center gap-3 mb-8 px-2 mt-2">
            <div className="p-2 bg-nature-600 rounded-lg shadow-lg shadow-nature-900/50">
                <Sprout size={24} className="text-white" />
            </div>
            <div>
                <h1 className="font-bold text-lg leading-none text-white tracking-tight">Double Good</h1>
                <h1 className="font-light text-sm text-nature-400 tracking-widest uppercase">Farming</h1>
            </div>
        </div>

        <div className="px-2 mb-2 text-[10px] font-bold text-slate-500 uppercase tracking-widest">{role} ACCESS</div>

        {/* PROCESSING STAFF MENU */}
        {(role === 'PROCESSING' || role === 'ADMIN') && (
          <>
            <button onClick={() => handleViewChange('RECEIVING')} className={`text-left p-3 rounded-lg font-bold flex items-center transition-all duration-200 ${currentView === 'RECEIVING' ? 'bg-nature-700 text-white shadow-lg shadow-nature-900/20 translate-x-1' : 'text-slate-400 hover:bg-slate-800 hover:text-white'}`}>
              <Package size={18} className="mr-3"/> Receiving Log
            </button>
            <button onClick={() => handleViewChange('PROCESSING')} className={`text-left p-3 rounded-lg font-bold flex items-center transition-all duration-200 ${currentView === 'PROCESSING' ? 'bg-nature-700 text-white shadow-lg shadow-nature-900/20 translate-x-1' : 'text-slate-400 hover:bg-slate-800 hover:text-white'}`}>
              <Utensils size={18} className="mr-3"/> Processing Floor
            </button>
          </>
        )}

        {/* PACKING MENU */}
        {(role === 'PACKING' || role === 'PROCESSING' || role === 'ADMIN') && (
           <button onClick={() => handleViewChange('PACKING')} className={`text-left p-3 rounded-lg font-bold flex items-center transition-all duration-200 ${currentView === 'PACKING' ? 'bg-nature-700 text-white shadow-lg shadow-nature-900/20 translate-x-1' : 'text-slate-400 hover:bg-slate-800 hover:text-white'}`}>
              <Box size={18} className="mr-3"/> Packing Station
           </button>
        )}

        {/* EVERYONE SEES INVENTORY */}
        <button onClick={() => handleViewChange('INVENTORY')} className={`text-left p-3 rounded-lg font-bold flex items-center transition-all duration-200 ${currentView === 'INVENTORY' ? 'bg-blue-700 text-white shadow-lg shadow-blue-900/20 translate-x-1' : 'text-slate-400 hover:bg-slate-800 hover:text-white'}`}>
           <LayoutGrid size={18} className="mr-3"/> Inventory
        </button>

        {/* SALES STAFF MENU */}
        {(role === 'SALES' || role === 'ADMIN') && (
           <>
             <button onClick={() => handleViewChange('SALES')} className={`text-left p-3 rounded-lg font-bold flex items-center transition-all duration-200 ${currentView === 'SALES' ? 'bg-nature-700 text-white shadow-lg shadow-nature-900/20 translate-x-1' : 'text-slate-400 hover:bg-slate-800 hover:text-white'}`}>
                <ShoppingCart size={18} className="mr-3"/> Sales / POS
             </button>
             <button onClick={() => handleViewChange('CRM')} className={`text-left p-3 rounded-lg font-bold flex items-center transition-all duration-200 ${currentView === 'CRM' ? 'bg-nature-700 text-white shadow-lg shadow-nature-900/20 translate-x-1' : 'text-slate-400 hover:bg-slate-800 hover:text-white'}`}>
                <Users size={18} className="mr-3"/> CRM
             </button>
           </>
        )}

        {/* ADMIN ONLY MENU */}
        {role === 'ADMIN' && (
           <button onClick={() => handleViewChange('FINANCE')} className={`text-left p-3 rounded-lg font-bold flex items-center transition-all duration-200 ${currentView === 'FINANCE' ? 'bg-slate-700 text-white shadow-lg shadow-slate-900/20 translate-x-1' : 'text-slate-400 hover:bg-slate-800 hover:text-white'}`}>
              <DollarSign size={18} className="mr-3"/> Full Finance
           </button>
        )}

        <button onClick={() => auth.signOut()} className="mt-auto text-left p-3 text-red-400 hover:bg-red-900/20 hover:text-red-300 rounded-lg flex items-center font-bold transition-colors">
          <LogOut size={18} className="mr-3"/> Sign Out
        </button>
      </nav>

      {/* MAIN CONTENT */}
      <main className="flex-1 overflow-hidden flex flex-col relative w-full">
        {/* MOBILE HEADER */}
        <header className="bg-white border-b border-slate-200 p-4 flex items-center justify-between lg:hidden shrink-0">
             <div className="flex items-center gap-3">
                <div className="p-1.5 bg-nature-600 rounded shadow-sm">
                    <Sprout size={18} className="text-white" />
                </div>
                <h1 className="font-bold text-slate-800">Double Good</h1>
             </div>
             <button onClick={() => setIsSidebarOpen(true)} className="p-2 text-slate-600 hover:bg-slate-100 rounded-lg">
                 <Menu size={24} />
             </button>
        </header>

        <div className="flex-1 overflow-auto p-4 md:p-6 lg:p-8">
            <div className="max-w-7xl mx-auto h-full">
                {currentView === 'PROCESSING' && <ProcessingPage />}
                {currentView === 'INVENTORY' && <InventoryPage />}
                {currentView === 'RECEIVING' && <ReceivingPage />}
                {currentView === 'PACKING' && <PackingPage />}
                
                {/* SALES & CRM VIEWS */}
                {currentView === 'SALES' && <FinancePage allowedTabs={['sales']} />}
                {currentView === 'CRM' && <CRMPage />}
                
                {currentView === 'FINANCE' && <FinancePage allowedTabs={['procurement', 'overview']} />}
                
                {currentView === 'DASHBOARD' && (
                    <div className="flex flex-col items-center justify-center h-full text-slate-400">
                        <LayoutDashboard size={48} className="mb-4 opacity-50"/>
                        <h2 className="text-xl font-bold">Select a module from the sidebar</h2>
                    </div>
                )}
            </div>
        </div>
      </main>
    </div>
  );
}
