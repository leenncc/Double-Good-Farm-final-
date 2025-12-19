import React, { useState, useEffect } from 'react';
import { getFinishedGoods, submitOnlineOrder, getRecipes } from '../services/sheetService';
import { FinishedGood, Recipe } from '../types';
import { ShoppingCart, Plus, Minus, ShoppingBag, X, CheckCircle, Store, ExternalLink, Info, Loader2 } from 'lucide-react';

export default function ShopPage() {
  const [goods, setGoods] = useState<FinishedGood[]>([]);
  const [recipes, setRecipes] = useState<Recipe[]>([]);
  const [cart, setCart] = useState<{item: FinishedGood, qty: number}[]>([]);
  const [isCartOpen, setIsCartOpen] = useState(false);
  const [loading, setLoading] = useState(true);
  
  // Product Detail Modal State
  const [selectedProduct, setSelectedProduct] = useState<FinishedGood | null>(null);
  const [selectedRecipe, setSelectedRecipe] = useState<Recipe | null>(null);
  
  // Checkout Form
  const [name, setName] = useState('');
  const [phone, setPhone] = useState('');
  const [email, setEmail] = useState('');
  const [address, setAddress] = useState('');
  const [orderSuccess, setOrderSuccess] = useState<string | null>(null);

  useEffect(() => {
    loadInventory();
    const interval = setInterval(loadInventory, 30000); // Live refresh
    return () => clearInterval(interval);
  }, []);

  const loadInventory = async () => {
    try {
        const [goodsRes, recipesRes] = await Promise.all([
            getFinishedGoods(true),
            getRecipes() // Fetch recipes for descriptions
        ]);

        if (goodsRes.success && goodsRes.data) {
            const activeStock = goodsRes.data.filter(g => g.quantity > 0);
            // Group logic to merge duplicate batches
            const grouped = activeStock.reduce((acc, curr) => {
                const key = `${curr.recipeName}|${curr.packagingType}`;
                if (!acc[key]) {
                    acc[key] = { ...curr };
                } else {
                    acc[key].quantity += curr.quantity;
                }
                
                // FIX: Ensure image persists if one batch has it but the accumulated one (usually newer) didn't
                if (curr.imageUrl && !acc[key].imageUrl) {
                    acc[key].imageUrl = curr.imageUrl;
                }
                
                return acc;
            }, {} as Record<string, FinishedGood>);
            setGoods(Object.values(grouped));
        }
        
        if (recipesRes) setRecipes(recipesRes);
    } catch (error) {
        console.error("Failed to load inventory:", error);
    } finally {
        setLoading(false);
    }
  };

  const handleProductClick = (product: FinishedGood) => {
      // Find matching recipe to show ingredients/notes
      const recipe = recipes.find(r => r.name === product.recipeName);
      setSelectedProduct(product);
      setSelectedRecipe(recipe || null);
  };

  const addToCart = (product: FinishedGood) => {
    setCart(prev => {
        const existing = prev.find(p => p.item.recipeName === product.recipeName && p.item.packagingType === product.packagingType);
        if (existing) {
            const newQty = Math.min(existing.qty + 1, product.quantity);
            return prev.map(p => p.item.id === existing.item.id ? { ...p, qty: newQty } : p);
        }
        return [...prev, { item: product, qty: 1 }];
    });
    setIsCartOpen(true);
    setSelectedProduct(null); // Close modal
  };

  const removeFromCart = (id: string) => {
      setCart(prev => prev.filter(p => p.item.id !== id));
  };

  const cartTotal = cart.reduce((sum, c) => sum + (c.item.sellingPrice * c.qty), 0);

  const handleSubmitOrder = async (e: React.FormEvent) => {
      e.preventDefault();
      if (cart.length === 0) return;
      
      const res = await submitOnlineOrder(name, phone, email, address, cart);
      
      if (res.success) {
          const orderText = cart.map(c => `- ${c.qty}x ${c.item.recipeName} (${c.item.packagingType})`).join('%0a');
          const message = `Hi, I would like to confirm my order #${res.data}:%0a${orderText}%0a*Total: RM ${cartTotal.toFixed(2)}*%0a%0aName: ${name}`;
          const whatsappUrl = `https://wa.me/60123456789?text=${message}`; 
          
          setOrderSuccess(whatsappUrl);
          setCart([]);
      } else {
          alert(res.message);
      }
  };

  if (orderSuccess) {
      return (
          <div className="min-h-screen bg-green-50 flex flex-col items-center justify-center p-6 text-center animate-in zoom-in">
              <CheckCircle size={64} className="text-green-600 mb-4" />
              <h1 className="text-3xl font-bold text-green-900 mb-2">Order Recorded!</h1>
              <p className="text-green-700 mb-6 max-w-md">Your order has been saved in our system. Please verify via WhatsApp to complete payment.</p>
              <a href={orderSuccess} target="_blank" rel="noreferrer" className="px-8 py-4 bg-green-600 text-white rounded-xl font-bold shadow-lg hover:bg-green-700 flex items-center transform hover:scale-105 transition-all">
                  <ExternalLink className="mr-2" /> Send to WhatsApp
              </a>
              <button onClick={() => window.location.reload()} className="mt-8 text-green-600 underline text-sm">Back to Shop</button>
          </div>
      );
  }

  return (
    <div className="h-full overflow-y-auto bg-slate-50 font-sans">
      <header className="bg-white sticky top-0 z-30 shadow-sm border-b border-slate-100">
          <div className="max-w-6xl mx-auto px-4 h-16 flex justify-between items-center">
              <div className="flex items-center text-earth-800">
                  <div className="bg-earth-100 p-2 rounded-lg mr-3"><Store size={24}/></div>
                  <div>
                      <h1 className="font-bold text-lg leading-tight">Double Good Farming</h1>
                      <p className="text-xs text-slate-500">Fresh from the Farm</p>
                  </div>
              </div>
              <button onClick={() => setIsCartOpen(true)} className="relative p-2 hover:bg-slate-50 rounded-full">
                  <ShoppingCart size={24} className="text-slate-700" />
                  {cart.length > 0 && (
                      <span className="absolute top-0 right-0 bg-red-500 text-white text-[10px] font-bold w-5 h-5 flex items-center justify-center rounded-full shadow-sm">
                          {cart.reduce((acc, c) => acc + c.qty, 0)}
                      </span>
                  )}
              </button>
          </div>
      </header>

      <div className="bg-earth-800 text-white py-12 px-4 text-center">
          <h2 className="text-3xl font-bold mb-2">Organic Mushrooms & Products</h2>
          <p className="opacity-80">Direct from our community growers to your table.</p>
      </div>

      <main className="max-w-6xl mx-auto px-4 py-8">
          {loading ? (
              <div className="text-center py-20 text-slate-400">Loading fresh produce...</div>
          ) : (
              <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-6">
                  {goods.map((product, index) => {
                      // Fallback: Check recipe for image if product doesn't have one
                      const relatedRecipe = recipes.find(r => r.name === product.recipeName);
                      const displayImage = product.imageUrl || relatedRecipe?.imageUrl;

                      return (
                          <div 
                            key={index} 
                            className="bg-white rounded-2xl shadow-sm border border-slate-100 overflow-hidden hover:shadow-md transition-shadow group cursor-pointer"
                            onClick={() => handleProductClick(product)}
                          >
                              <div className="h-48 bg-slate-200 relative overflow-hidden">
                                  {displayImage ? (
                                      <img src={displayImage} className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-500" alt={product.recipeName} />
                                  ) : (
                                      <div className="w-full h-full flex items-center justify-center text-slate-400"><Store size={40}/></div>
                                  )}
                                  <div className="absolute top-3 right-3 bg-white/90 backdrop-blur-sm px-2 py-1 rounded text-xs font-bold text-slate-700 shadow-sm">
                                      {product.quantity} left
                                  </div>
                                  <div className="absolute bottom-2 left-2 bg-black/60 text-white text-[10px] px-2 py-1 rounded font-bold uppercase backdrop-blur-sm flex items-center">
                                      <Info size={10} className="mr-1"/> Details
                                  </div>
                              </div>
                              <div className="p-5">
                                  <div className="mb-4">
                                      <h3 className="font-bold text-lg text-slate-900 leading-tight mb-1">{product.recipeName}</h3>
                                      <p className="text-sm text-slate-500 uppercase tracking-wider font-bold">{product.packagingType}</p>
                                  </div>
                                  <div className="flex justify-between items-center">
                                      <span className="text-xl font-black text-green-700">RM {product.sellingPrice.toFixed(2)}</span>
                                      <button 
                                        onClick={(e) => { e.stopPropagation(); addToCart(product); }}
                                        className="bg-earth-800 text-white px-4 py-2 rounded-lg font-bold text-sm hover:bg-earth-900 transition-colors shadow-sm"
                                      >
                                          Add to Cart
                                      </button>
                                  </div>
                              </div>
                          </div>
                      );
                  })}
                  {goods.length === 0 && (
                      <div className="col-span-full text-center py-20 bg-white rounded-2xl border border-dashed border-slate-300">
                          <p className="text-slate-500 text-lg">Everything is sold out! Check back later.</p>
                      </div>
                  )}
              </div>
          )}
      </main>

      {/* PRODUCT DETAIL MODAL */}
      {selectedProduct && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4 animate-in fade-in">
              <div className="bg-white rounded-2xl shadow-2xl max-w-lg w-full overflow-hidden relative flex flex-col max-h-[90vh]">
                  <button onClick={() => setSelectedProduct(null)} className="absolute top-4 right-4 z-10 p-2 bg-white/80 hover:bg-white rounded-full shadow-sm"><X size={20}/></button>
                  
                  <div className="h-64 bg-slate-200 relative">
                      {(() => {
                         const relatedRecipe = recipes.find(r => r.name === selectedProduct.recipeName);
                         const displayImage = selectedProduct.imageUrl || relatedRecipe?.imageUrl;
                         return displayImage ? (
                              <img src={displayImage} className="w-full h-full object-cover" />
                          ) : (
                              <div className="w-full h-full flex items-center justify-center text-slate-400"><Store size={48}/></div>
                          );
                      })()}
                  </div>
                  
                  <div className="p-6 overflow-y-auto">
                      <div className="flex justify-between items-start mb-4">
                          <div>
                              <h2 className="text-2xl font-bold text-slate-800">{selectedProduct.recipeName}</h2>
                              <span className="px-2 py-1 bg-slate-100 text-slate-600 text-xs font-bold uppercase rounded mt-1 inline-block">{selectedProduct.packagingType}</span>
                          </div>
                          <span className="text-2xl font-black text-green-700">RM {selectedProduct.sellingPrice.toFixed(2)}</span>
                      </div>

                      {selectedRecipe ? (
                          <div className="space-y-4">
                              <div className="bg-earth-50 p-4 rounded-xl border border-earth-100">
                                  <h4 className="text-xs font-bold text-earth-800 uppercase tracking-wide mb-2 flex items-center"><Info size={14} className="mr-1"/> About this Product</h4>
                                  <p className="text-slate-700 text-sm leading-relaxed">{selectedRecipe.notes || "Freshly harvested and packed with care."}</p>
                              </div>

                              {selectedRecipe.ingredients && (
                                  <div className="bg-slate-50 p-4 rounded-xl border border-slate-100">
                                      <h4 className="text-xs font-bold text-slate-500 uppercase tracking-wide mb-2">Ingredients</h4>
                                      <p className="text-slate-700 text-sm leading-relaxed">{selectedRecipe.ingredients}</p>
                                  </div>
                              )}
                          </div>
                      ) : (
                          <p className="text-slate-400 text-sm italic">Standard produce. No specific recipe details available.</p>
                      )}
                  </div>

                  <div className="p-4 border-t bg-slate-50">
                      <button 
                          onClick={() => addToCart(selectedProduct)}
                          className="w-full py-4 bg-earth-800 text-white rounded-xl font-bold text-lg hover:bg-earth-900 transition-all shadow-lg flex items-center justify-center"
                      >
                          <ShoppingCart className="mr-2"/> Add to Cart
                      </button>
                  </div>
              </div>
          </div>
      )}

      {/* CART DRAWER */}
      {isCartOpen && (
          <div className="fixed inset-0 z-50 flex justify-end">
              <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={() => setIsCartOpen(false)}></div>
              <div className="relative w-full max-w-md bg-white h-full shadow-2xl flex flex-col animate-in slide-in-from-right">
                  <div className="p-6 border-b border-slate-100 flex justify-between items-center bg-slate-50">
                      <h2 className="font-bold text-xl flex items-center"><ShoppingBag className="mr-2"/> Your Basket</h2>
                      <button onClick={() => setIsCartOpen(false)} className="p-2 hover:bg-slate-200 rounded-full"><X size={20}/></button>
                  </div>
                  
                  <div className="flex-1 overflow-y-auto p-6 space-y-6">
                      {cart.length === 0 ? (
                          <div className="text-center text-slate-400 py-10">Your basket is empty.</div>
                      ) : (
                          cart.map((item, i) => {
                              // Display image logic for cart
                              const relatedRecipe = recipes.find(r => r.name === item.item.recipeName);
                              const displayImage = item.item.imageUrl || relatedRecipe?.imageUrl;
                              
                              return (
                              <div key={i} className="flex gap-4 border-b border-slate-50 pb-4 last:border-0">
                                  <div className="w-20 h-20 bg-slate-100 rounded-lg overflow-hidden flex-shrink-0">
                                      {displayImage && <img src={displayImage} className="w-full h-full object-cover"/>}
                                  </div>
                                  <div className="flex-1">
                                      <h4 className="font-bold text-slate-800">{item.item.recipeName}</h4>
                                      <p className="text-xs text-slate-500 mb-2">{item.item.packagingType}</p>
                                      <div className="flex justify-between items-center">
                                          <p className="font-bold text-green-700">RM {(item.item.sellingPrice * item.qty).toFixed(2)}</p>
                                          <div className="flex items-center bg-slate-100 rounded-lg">
                                              <button onClick={() => removeFromCart(item.item.id)} className="p-1 px-2 text-slate-500 hover:text-red-500"><X size={14}/></button>
                                              <span className="px-2 text-sm font-bold">{item.qty}</span>
                                          </div>
                                      </div>
                                  </div>
                              </div>
                          )})
                      )}
                  </div>

                  {cart.length > 0 && (
                      <div className="p-6 border-t border-slate-100 bg-slate-50">
                          <div className="flex justify-between items-center mb-6 text-lg font-bold text-slate-800">
                              <span>Total</span>
                              <span>RM {cartTotal.toFixed(2)}</span>
                          </div>
                          
                          <form onSubmit={handleSubmitOrder} className="space-y-3">
                              <input required placeholder="Your Name" className="w-full p-3 border rounded-xl" value={name} onChange={e => setName(e.target.value)} />
                              <input required placeholder="WhatsApp Number" className="w-full p-3 border rounded-xl" value={phone} onChange={e => setPhone(e.target.value)} />
                              <input required type="email" placeholder="Email Address" className="w-full p-3 border rounded-xl" value={email} onChange={e => setEmail(e.target.value)} />
                              <textarea required placeholder="Delivery Address" className="w-full p-3 border rounded-xl" value={address} onChange={e => setAddress(e.target.value)} rows={2} />
                              
                              <button className="w-full py-4 bg-green-600 text-white font-bold rounded-xl shadow-lg hover:bg-green-700 hover:shadow-xl transition-all">
                                  Confirm Order
                              </button>
                          </form>
                      </div>
                  )}
              </div>
          </div>
      )}
    </div>
  );
}