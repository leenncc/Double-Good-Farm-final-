import React, { useState, useEffect } from 'react';
import { Truck, Scale, AlertTriangle, CheckCircle2, Save, Bell, ArrowRight, RefreshCw, Clock, Box, Sprout, Hash, Fingerprint } from 'lucide-react';
import { createBatch } from '../services/sheetService';
import { db } from '../services/firebase';

// --- COUNTDOWN HELPER COMPONENT ---
const CountdownTimer = ({ targetDate }: { targetDate: string }) => {
    const [timeLeft, setTimeLeft] = useState('');

    useEffect(() => {
        const calculateTime = () => {
            const now = new Date().getTime();
            const target = new Date(targetDate).getTime();
            const difference = target - now;
            
            if (difference > 0) {
                const hours = Math.floor((difference / (1000 * 60 * 60)));
                const minutes = Math.floor((difference % (1000 * 60 * 60)) / (1000 * 60));
                setTimeLeft(`${hours}h ${minutes}m remaining`);
            } else {
                setTimeLeft('Arriving now / Overdue');
            }
        };

        calculateTime();
        const timer = setInterval(calculateTime, 60000); 
        return () => clearInterval(timer);
    }, [targetDate]);

    return (
        <span className={`text-xs font-bold px-2 py-1 rounded-full ${timeLeft.includes('Overdue') ? 'bg-red-100 text-red-700' : 'bg-blue-100 text-blue-700'}`}>
            {timeLeft}
        </span>
    );
};

interface ReceivingPageProps {
  prefillData?: any; 
  onClear?: () => void;
}

const ReceivingPage: React.FC<ReceivingPageProps> = ({ prefillData, onClear }) => {
  const [alerts, setAlerts] = useState<any[]>([]);
  const [activeAlertId, setActiveAlertId] = useState<string | null>(null);

  const [formData, setFormData] = useState({
    batchId: '', entityId: '', sourceFarm: '', species: '', 
    flushNumber: '', rawWeight: '', spoiledWeight: '0'
  });

  const [status, setStatus] = useState<'idle' | 'submitting' | 'success' | 'error'>('idle');
  const [isLoadingAlerts, setIsLoadingAlerts] = useState(false);

  // --- REAL-TIME LISTENER ---
  useEffect(() => {
     setIsLoadingAlerts(true);
     
     // Using existing 'db' (firebase compat)
     const unsubscribe = db.collection("mn_delivery_orders")
        .where("status", "==", "IN_TRANSIT")
        .onSnapshot((snapshot) => {
            const incomingAlerts: any[] = [];
            
            snapshot.forEach((doc) => {
                const data = doc.data();
                
                // --- 2-HOUR RULE LOGIC ---
                // 1. Get sent time (safely handle Firestore Timestamp vs String)
                let sentTime = new Date();
                if (data.createdAt) {
                    if (typeof data.createdAt.toDate === 'function') {
                        sentTime = data.createdAt.toDate();
                    } else {
                        sentTime = new Date(data.createdAt);
                    }
                }
                
                // 2. Add 2 Hours to Sent Time
                const arrivalTime = new Date(sentTime.getTime() + (2 * 60 * 60 * 1000));
                
                incomingAlerts.push({
                    id: doc.id, 
                    batchId: data.batchId, 
                    entityId: data.entityId,
                    farmName: data.entityId === 'ent_001' ? 'Green Spore Co-op' : (data.sourceFarm || 'MyceliumNexus Farm'), 
                    estimatedWeightKg: data.estimatedYield, 
                    species: data.species,
                    flushNumber: data.flushNumber || '1',
                    timestamp: sentTime.toISOString(),
                    deliveryDate: arrivalTime.toISOString() // Pass calculated limit
                });
            });

            setAlerts(incomingAlerts);
            setIsLoadingAlerts(false);
        }, (error) => {
            console.error("Listener Error:", error);
            setIsLoadingAlerts(false);
        });

     return () => unsubscribe();
  }, []); 

  // --- PRE-FILL (Does NOT update DB yet) ---
  const handlePreFill = (alertData: any) => {
      setFormData({
          batchId: alertData.batchId || 'Unknown Batch',
          entityId: alertData.entityId || '',
          sourceFarm: alertData.farmName || '',
          species: alertData.species || '',
          flushNumber: String(alertData.flushNumber || '1'),
          rawWeight: String(alertData.estimatedWeightKg || ''),
          spoiledWeight: '0'
      });
      setActiveAlertId(alertData.id); // Store ID to close later
      window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
  };

  const netWeight = (parseFloat(formData.rawWeight) || 0) - (parseFloat(formData.spoiledWeight) || 0);
  const isSpoiledDetected = (parseFloat(formData.spoiledWeight) || 0) > 0;

  // --- SUBMIT: SAVE & CLOSE ALERT ---
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.sourceFarm || !formData.rawWeight) return;

    // --- NEW VALIDATION RULE ---
    const raw = parseFloat(formData.rawWeight) || 0;
    const spoiled = parseFloat(formData.spoiledWeight) || 0;
    if (spoiled > raw) {
        alert("Error: Spoiled weight cannot exceed total weight.");
        return;
    }
    
    setStatus('submitting');
    
    // 1. Save to Inventory (createBatch updated to accept new fields)
    const result = await createBatch(
      formData.sourceFarm, 
      parseFloat(formData.rawWeight), 
      parseFloat(formData.spoiledWeight),
      formData.batchId,    // Farm Batch ID
      formData.species,    // Species
      formData.flushNumber // Flush #
    );

    if (result.success) {
      // 2. IF SUCCESSFUL, Update Database to 'DELIVERED'
      if (activeAlertId) {
          try {
              await db.collection("mn_delivery_orders").doc(activeAlertId).update({ status: "DELIVERED" });
              setAlerts(prev => prev.filter(a => a.id !== activeAlertId));
          } catch (err) {
              console.error("Could not update status:", err);
          }
      }

      setStatus('success');
      setFormData({ batchId: '', entityId: '', sourceFarm: '', species: '', flushNumber: '', rawWeight: '', spoiledWeight: '0' });
      setActiveAlertId(null);
      if (onClear) onClear(); 
      setTimeout(() => setStatus('idle'), 3000);
    } else {
      setStatus('error');
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center space-x-4">
        <div className="p-3 bg-earth-100 rounded-xl text-earth-700">
          <Truck size={32} />
        </div>
        <div>
          <h2 className="text-2xl font-bold text-slate-900">Receiving Log</h2>
          <p className="text-slate-500">Log incoming mushroom batches from farms</p>
        </div>
      </div>

      <div className="space-y-3">
         <div className="flex items-center justify-between text-sm font-bold text-slate-500 uppercase tracking-wider">
             <span className="flex items-center gap-2">
                 Incoming Shipments ({alerts.length})
                 <span className="relative flex h-2 w-2">
                   <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                   <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
                 </span>
             </span>
             {isLoadingAlerts && <RefreshCw size={14} className="animate-spin text-slate-400" />}
         </div>
         
         {alerts.length > 0 ? (
             <div className="space-y-3 animate-in slide-in-from-top duration-500">
                 {alerts.map(alert => (
                     <div key={alert.id} className={`bg-orange-50 border border-orange-200 rounded-xl p-4 flex flex-col md:flex-row justify-between items-start md:items-center shadow-sm relative overflow-hidden transition-all ${activeAlertId === alert.id ? 'ring-2 ring-orange-400 bg-orange-100' : ''}`}>
                         <div className="absolute top-0 right-0 p-2 opacity-10 pointer-events-none">
                             <Truck size={100} />
                         </div>
                         <div className="flex items-start space-x-4 mb-4 md:mb-0 z-10">
                             <div className="p-3 bg-white rounded-lg text-orange-600 shadow-sm animate-pulse">
                                 <Bell size={24} />
                             </div>
                             <div>
                                 <div className="flex items-center gap-2 mb-1">
                                     <h4 className="font-bold text-orange-900 text-lg">Incoming Shipment</h4>
                                     <CountdownTimer targetDate={alert.deliveryDate} />
                                 </div>
                                 <div className="text-orange-800 text-sm space-y-1">
                                     <p className="font-semibold flex items-center gap-2">
                                         {alert.farmName} 
                                         <span className="text-xs bg-orange-200 px-1.5 py-0.5 rounded text-orange-800">
                                            {alert.estimatedWeightKg} kg
                                         </span>
                                     </p>
                                     <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs mt-2 opacity-90">
                                         <span className="flex items-center gap-1"><Box size={12}/> ID: {alert.batchId}</span>
                                         <span className="flex items-center gap-1"><Sprout size={12}/> {alert.species}</span>
                                         <span className="flex items-center gap-1"><Hash size={12}/> Flush #{alert.flushNumber}</span>
                                         <span className="flex items-center gap-1"><Clock size={12}/> Sent: {new Date(alert.timestamp).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})}</span>
                                     </div>
                                 </div>
                             </div>
                         </div>
                         
                         {activeAlertId === alert.id ? (
                             <div className="z-10 px-6 py-3 bg-white border border-orange-200 text-orange-600 font-bold rounded-lg shadow-sm flex items-center whitespace-nowrap cursor-default">
                                 <CheckCircle2 size={18} className="mr-2" /> Selected
                             </div>
                         ) : (
                             <button 
                                onClick={() => handlePreFill(alert)}
                                className="z-10 px-6 py-3 bg-orange-600 hover:bg-orange-700 text-white font-bold rounded-lg shadow-md transition-all flex items-center whitespace-nowrap"
                             >
                                Accept & Pre-fill <ArrowRight size={18} className="ml-2" />
                             </button>
                         )}
                     </div>
                 ))}
             </div>
         ) : (
             <div className="bg-slate-50 border border-slate-200 border-dashed rounded-xl p-6 text-center text-slate-400">
                 <p>No active shipments found.</p>
                 <p className="text-xs mt-1">Listening for 'IN_TRANSIT' status...</p>
             </div>
         )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 pt-4">
        <form onSubmit={handleSubmit} className="bg-white p-6 rounded-2xl shadow-sm border border-slate-200 space-y-6">
          <div className="flex justify-between items-center border-b border-slate-100 pb-4 mb-2">
             <h3 className="font-bold text-slate-800">New Batch Entry</h3>
             {activeAlertId && <span className="text-xs bg-blue-50 text-blue-600 px-2 py-1 rounded font-bold animate-pulse">Linked to Real-Time Alert</span>}
          </div>

          <div className="bg-slate-50 p-4 rounded-xl border border-slate-200 space-y-4">
              <div className="grid grid-cols-2 gap-4">
                  <div>
                      <label className="block text-xs font-bold text-slate-500 uppercase mb-1">Batch ID</label>
                      <div className="relative">
                          <input 
                            type="text" 
                            readOnly={!!activeAlertId} 
                            value={formData.batchId}
                            onChange={(e) => setFormData({...formData, batchId: e.target.value})}
                            className={`w-full rounded-lg border p-2 pl-8 text-sm font-mono ${activeAlertId ? 'bg-slate-100 text-slate-500 border-transparent' : 'bg-white border-slate-300'}`}
                            placeholder="Manual ID..."
                          />
                          <Fingerprint className="absolute left-2.5 top-2.5 text-slate-400" size={14} />
                      </div>
                  </div>
                  <div>
                      <label className="block text-xs font-bold text-slate-500 uppercase mb-1">Flush #</label>
                      <div className="relative">
                          <input 
                            type="text" 
                            readOnly={!!activeAlertId}
                            value={formData.flushNumber}
                            onChange={(e) => setFormData({...formData, flushNumber: e.target.value})}
                            className={`w-full rounded-lg border p-2 pl-8 text-sm ${activeAlertId ? 'bg-slate-100 text-slate-500 border-transparent' : 'bg-white border-slate-300'}`}
                            placeholder="1"
                          />
                          <Hash className="absolute left-2.5 top-2.5 text-slate-400" size={14} />
                      </div>
                  </div>
              </div>
              
              <div>
                  <label className="block text-xs font-bold text-slate-500 uppercase mb-1">Mushroom Species</label>
                  <div className="relative">
                      <input 
                        type="text" 
                        readOnly={!!activeAlertId}
                        value={formData.species}
                        onChange={(e) => setFormData({...formData, species: e.target.value})}
                        className={`w-full rounded-lg border p-2 pl-8 text-sm ${activeAlertId ? 'bg-slate-100 text-slate-500 border-transparent' : 'bg-white border-slate-300'}`}
                        placeholder="e.g. Grey Oyster"
                      />
                      <Sprout className="absolute left-2.5 top-2.5 text-slate-400" size={14} />
                  </div>
              </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-slate-700 mb-2">Source Farm</label>
            {activeAlertId ? (
                <div className="w-full rounded-lg border border-slate-200 bg-slate-100 p-3 text-slate-600 font-medium flex justify-between items-center">
                    {formData.sourceFarm}
                    <span className="text-[10px] text-slate-400 font-mono">ID: {formData.entityId}</span>
                </div>
            ) : (
                <select
                  value={formData.sourceFarm}
                  onChange={(e) => setFormData({...formData, sourceFarm: e.target.value})}
                  className="w-full rounded-lg border-slate-300 border p-3 focus:ring-2 focus:ring-nature-500 focus:border-nature-500 outline-none bg-white"
                  required
                >
                  <option value="">Select a Farm...</option>
                  <option value="Green Valley Farms">Green Valley Farms</option>
                  <option value="Hilltop Myco">Hilltop Myco</option>
                  <option value="Forest Floor Organics">Forest Floor Organics</option>
                </select>
            )}
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">Total Weight (kg)</label>
              <div className="relative">
                <input
                  type="number"
                  step="0.1"
                  min="0"
                  value={formData.rawWeight}
                  onChange={(e) => setFormData({...formData, rawWeight: e.target.value})}
                  className="w-full rounded-lg border-slate-300 border p-3 pl-10 focus:ring-2 focus:ring-nature-500 focus:border-nature-500 outline-none"
                  placeholder="0.00"
                  required
                />
                <Scale className="absolute left-3 top-3.5 text-slate-400" size={18} />
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">Spoiled Weight (kg)</label>
              <div className="relative">
                <input
                  type="number"
                  step="0.1"
                  min="0"
                  value={formData.spoiledWeight}
                  onChange={(e) => setFormData({...formData, spoiledWeight: e.target.value})}
                  className="w-full rounded-lg border-slate-300 border p-3 pl-10 focus:ring-2 focus:ring-red-500 focus:border-red-500 outline-none"
                  placeholder="0.00"
                />
                <AlertTriangle className="absolute left-3 top-3.5 text-slate-400" size={18} />
              </div>
            </div>
          </div>

          <div className="bg-slate-50 p-4 rounded-xl border border-slate-200">
            <div className="flex justify-between items-end">
              <span className="text-slate-500 font-medium">Net Usable Weight</span>
              <span className="text-3xl font-bold text-nature-700">{netWeight.toFixed(2)} <span className="text-base text-slate-400 font-normal">kg</span></span>
            </div>
            {isSpoiledDetected && (
              <div className="mt-2 text-xs text-red-600 flex items-center">
                <AlertTriangle size={12} className="mr-1" />
                {((parseFloat(formData.spoiledWeight) / parseFloat(formData.rawWeight)) * 100).toFixed(1)}% spoilage detected
              </div>
            )}
          </div>

          <button
            type="submit"
            disabled={status === 'submitting'}
            className={`w-full py-4 rounded-xl font-bold text-lg flex items-center justify-center transition-all ${
              status === 'submitting' 
                ? 'bg-slate-100 text-slate-400 cursor-not-allowed'
                : 'bg-earth-800 hover:bg-earth-900 text-white shadow-lg hover:shadow-xl'
            }`}
          >
            {status === 'submitting' ? (
              'Processing...'
            ) : status === 'success' ? (
              <span className="flex items-center"><CheckCircle2 className="mr-2" /> Logged Successfully</span>
            ) : (
              <span className="flex items-center"><Save className="mr-2" /> Log Delivery</span>
            )}
          </button>
        </form>

        <div className="hidden lg:block bg-earth-50 rounded-2xl p-6 border border-earth-100 h-fit">
          <h3 className="font-semibold text-earth-800 mb-4">Reception Protocols</h3>
          <ul className="space-y-4 text-sm text-earth-700">
            <li className="flex items-start">
              <span className="flex-shrink-0 w-6 h-6 rounded-full bg-white border border-earth-200 text-earth-600 flex items-center justify-center mr-3 font-bold text-xs">1</span>
              Verify delivery truck seal and documentation match.
            </li>
            <li className="flex items-start">
              <span className="flex-shrink-0 w-6 h-6 rounded-full bg-white border border-earth-200 text-earth-600 flex items-center justify-center mr-3 font-bold text-xs">2</span>
              Confirm <strong>Batch ID</strong> and <strong>Species</strong> match the manifest.
            </li>
            <li className="flex items-start">
              <span className="flex-shrink-0 w-6 h-6 rounded-full bg-white border border-earth-200 text-earth-600 flex items-center justify-center mr-3 font-bold text-xs">3</span>
              Perform gross weight check.
            </li>
            <li className="flex items-start">
              <span className="flex-shrink-0 w-6 h-6 rounded-full bg-white border border-earth-200 text-earth-600 flex items-center justify-center mr-3 font-bold text-xs">4</span>
              Click <strong>Accept & Pre-fill</strong> on the alert card to auto-log.
            </li>
          </ul>
        </div>
      </div>
    </div>
  );
};

export default ReceivingPage;