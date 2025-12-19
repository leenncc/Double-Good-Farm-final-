import React, { useEffect, useState, useRef } from 'react';
import { fetchBatches, updateBatchStatus, getRecipes, saveRecipe, deleteRecipe } from '../services/sheetService';
import { MushroomBatch, BatchStatus, Recipe, ProcessConfig } from '../types';
import { ThermometerSun, CheckCircle, Clock, Play, RotateCcw, Utensils, BookOpen, Plus, Trash2, ChefHat, Flame, Image as ImageIcon, X, Waves, Wind, ShieldAlert, Upload, Pencil, FastForward, AlertTriangle, RefreshCw, CopyMinus, Scale, ArrowRight, LayoutGrid, Cloud, WifiOff, FileText, List, AlertCircle } from 'lucide-react';
import { auth } from '../services/firebase';

const DEFAULT_RECIPES: Recipe[] = [];

const useRecipes = () => {
  const [recipes, setRecipes] = useState<Recipe[]>([]);
  const [isCloud, setIsCloud] = useState(false);
  const [loading, setLoading] = useState(true);
  const [isAuthReady, setIsAuthReady] = useState(false);

  const loadRecipes = async () => {
      setLoading(true);
      await new Promise(r => setTimeout(r, 500)); 
      const data = await getRecipes();
      const hasSeeded = localStorage.getItem('shroomtrack_recipes_seeded_init'); 
      
      if (data.length === 0 && !hasSeeded && DEFAULT_RECIPES.length > 0) {
          await Promise.all(DEFAULT_RECIPES.map(r => saveRecipe(r)));
          localStorage.setItem('shroomtrack_recipes_seeded_init', 'true');
          const newData = await getRecipes();
          setRecipes(newData.length > 0 ? newData : DEFAULT_RECIPES);
      } else {
          setRecipes(data);
      }
      setLoading(false);
  };

  useEffect(() => {
    const unsubscribe = auth.onAuthStateChanged(async (user) => {
        if (user) {
            setIsCloud(true);
            setIsAuthReady(true);
            loadRecipes();
        } else {
            setIsCloud(false);
            setIsAuthReady(true);
            loadRecipes();
        }
    });
    return () => unsubscribe();
  }, []);

  const handleAddRecipe = async (recipe: Recipe, file?: File) => {
    if (!isAuthReady) return;
    await saveRecipe(recipe, file);
    loadRecipes();
  };

  const handleUpdateRecipe = async (updatedRecipe: Recipe, file?: File) => {
    if (!isAuthReady) return;
    await saveRecipe(updatedRecipe, file);
    loadRecipes();
  };

  const handleDeleteRecipe = async (id: string) => {
    if (!isAuthReady) return;
    if (!window.confirm("Are you sure you want to delete this recipe?")) return;
    setRecipes(prev => prev.filter(r => r.id !== id));
    await deleteRecipe(id);
    loadRecipes();
  };

  const handleRemoveDuplicates = async () => {
      if (!isAuthReady) return;
      if (!window.confirm("This will find recipes with the exact same name and delete the extras, keeping only one of each. Continue?")) return;
      
      try {
          const currentRecipes = await getRecipes();
          const seenNames = new Set<string>();
          const toDelete: string[] = [];
          const unique: Recipe[] = [];

          for (const r of currentRecipes) {
              // FIX: Defensive check for r.name to prevent trim/toLowerCase on undefined
              const normalizedName = (r.name || '').trim().toLowerCase();
              if (seenNames.has(normalizedName)) {
                  toDelete.push(r.id);
              } else {
                  seenNames.add(normalizedName);
                  unique.push(r);
              }
          }

          if (toDelete.length === 0) {
              alert("No duplicates found.");
              return;
          }

          setRecipes(unique);
          const deletePromises = toDelete.map(id => deleteRecipe(id).catch(() => false));
          await Promise.all(deletePromises);
          alert(`Cleanup complete. Removed ${toDelete.length} duplicate recipes.`);
          loadRecipes();
      } catch (error) {
          console.error("Error removing duplicates:", error);
      }
  };

  return { recipes, isCloud, loading, addRecipe: handleAddRecipe, updateRecipe: handleUpdateRecipe, deleteRecipe: handleDeleteRecipe, removeDuplicates: handleRemoveDuplicates };
};

const useNow = () => {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const interval = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(interval);
  }, []);
  return now;
};

const PersistentBatchCard: React.FC<{
  batch: MushroomBatch;
  recipes: Recipe[];
  onStart: (id: string, recipe: Recipe) => void;
  onComplete: (id: string, wastage: number, reason?: string) => void;
  onSwitchRecipe: (id: string, newRecipe: Recipe) => void;
  onSpeedUp: (id: string) => void;
}> = ({ batch, recipes, onStart, onComplete, onSwitchRecipe, onSpeedUp }) => {
  const isProcessing = batch.status === BatchStatus.PROCESSING && batch.processConfig;
  const now = useNow();
  
  const [qcGoodWeight, setQcGoodWeight] = useState<string>('');
  const [qcWastageWeight, setQcWastageWeight] = useState<string>('0');
  const [wastageReason, setWastageReason] = useState<string>('');
  const [customReason, setCustomReason] = useState<string>('');
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  
  const [showModal, setShowModal] = useState(false);
  const [showSwitchModal, setShowSwitchModal] = useState(false);
  const activeRecipe = recipes.find(r => r.name === batch.selectedRecipeName) || recipes[0] || { name: 'Unknown', type: 'CHIPS', imageUrl: '' };

  let stage: 'WASH' | 'DRAIN' | 'COOK' | 'COMPLETE' = 'WASH';
  let timeLeft = 0;
  let progress = 0;
  let label = '';
  let warning = '';
  let icon = null;
  let colorClass = '';

  if (isProcessing && batch.processConfig) {
    const { startTime, washDurationSeconds, drainDurationSeconds, cookDurationSeconds } = batch.processConfig;
    const elapsedSeconds = Math.floor((now - startTime) / 1000);
    const drainStart = washDurationSeconds;
    const cookStart = washDurationSeconds + drainDurationSeconds;
    const finishTime = cookStart + cookDurationSeconds;

    if (elapsedSeconds < drainStart) {
      stage = 'WASH';
      timeLeft = washDurationSeconds - elapsedSeconds;
      progress = (elapsedSeconds / washDurationSeconds) * 100;
      label = 'WASHING CYCLE';
      warning = 'CAUTION: ROTATING DRUM ACTIVE';
      icon = <Waves className="animate-bounce" size={32} />;
      colorClass = 'bg-blue-500';
    } else if (elapsedSeconds < cookStart) {
      stage = 'DRAIN';
      timeLeft = (washDurationSeconds + drainDurationSeconds) - elapsedSeconds;
      progress = ((elapsedSeconds - drainStart) / drainDurationSeconds) * 100;
      label = 'DRAINING / AIR DRY';
      warning = 'HIGH VELOCITY AIRFLOW';
      icon = <Wind className="animate-pulse" size={32} />;
      colorClass = 'bg-cyan-500';
    } else if (elapsedSeconds < finishTime) {
      stage = 'COOK';
      timeLeft = finishTime - elapsedSeconds;
      progress = ((elapsedSeconds - cookStart) / cookDurationSeconds) * 100;
      label = activeRecipe.type === 'CHIPS' ? 'FRYING PROCESS' : 'DEHYDRATION';
      warning = activeRecipe.type === 'CHIPS' ? 'DANGER: HOT OIL 160°C' : 'HEAT CHAMBER SEALED';
      icon = <Flame className={activeRecipe.type === 'CHIPS' ? 'animate-pulse' : ''} size={32} />;
      colorClass = activeRecipe.type === 'CHIPS' ? 'bg-orange-600' : 'bg-amber-500';
    } else {
      stage = 'COMPLETE';
      timeLeft = 0;
      progress = 100;
      label = 'CYCLE COMPLETE';
      warning = 'SAFE TO UNLOAD';
      icon = <CheckCircle size={32} />;
      colorClass = 'bg-nature-600';
    }
  }

  const formatTime = (seconds: number) => {
    const m = Math.floor(seconds / 60);
    const s = Math.floor(seconds % 60);
    return `${m}:${s < 10 ? '0' : ''}${s}`;
  };

  const handleFinishBatch = () => {
      setErrorMsg(null);
      const good = parseFloat(qcGoodWeight) || 0;
      const waste = parseFloat(qcWastageWeight) || 0;
      const totalInput = batch.remainingWeightKg || batch.netWeightKg;

      if (Math.abs((good + waste) - totalInput) > 0.1) {
          setErrorMsg(`Total weight (${(good + waste).toFixed(2)}kg) must match Input (${totalInput.toFixed(2)}kg)`);
          return;
      }

      const finalReason = wastageReason === 'Other' ? `Other: ${customReason}` : wastageReason;
      onComplete(batch.id, waste, finalReason);
  };

  return (
    <div className={`bg-white rounded-xl shadow-sm border overflow-hidden flex flex-col transition-all ${isProcessing ? 'border-orange-200 ring-1 ring-orange-100' : 'border-slate-200'}`}>
      <div className="p-4 border-b border-slate-50 flex justify-between items-start">
        <div>
          <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Batch {batch.id}</span>
          <h3 className="font-bold text-slate-800">{batch.sourceFarm}</h3>
          <p className="text-sm font-semibold text-slate-700 mt-1">{batch.remainingWeightKg || batch.netWeightKg} kg <span className="text-xs font-normal text-slate-400">Input</span></p>
        </div>
        <div className={`p-2 rounded-full ${isProcessing ? 'bg-orange-50 text-orange-600' : 'bg-slate-50 text-slate-400'}`}>
          <Utensils size={18} />
        </div>
      </div>
      <div className="p-4 flex-1 flex flex-col justify-end">
        {isProcessing ? (
          <div className="space-y-4">
             <div className={`relative rounded-xl overflow-hidden text-white p-5 text-center transition-colors duration-500 ${colorClass}`}>
                <div className="relative z-10 flex flex-col items-center">
                   {stage !== 'COMPLETE' && (
                     <div className="mb-2 px-2 py-0.5 bg-black/30 rounded text-[9px] font-bold text-yellow-300 animate-pulse flex items-center">
                       <ShieldAlert size={10} className="mr-1"/> {warning}
                     </div>
                   )}
                   <div className="mb-2 opacity-90">{icon}</div>
                   <h4 className="text-sm font-bold tracking-widest mb-1">{label}</h4>
                   {stage !== 'COMPLETE' ? (
                     <div className="flex items-center justify-center space-x-3 my-2">
                        <div className="text-4xl font-black font-mono drop-shadow-md tabular-nums">{formatTime(timeLeft)}</div>
                        <button onClick={() => onSpeedUp(batch.id)} className="p-2 bg-white/30 hover:bg-white/50 rounded-full text-white transition-all transform hover:scale-110 active:scale-95"><FastForward size={20} fill="currentColor" /></button>
                     </div>
                   ) : (
                     <div className="text-lg font-bold my-2">Ready for QC</div>
                   )}
                   {stage !== 'COMPLETE' && (
                     <div className="w-full bg-black/20 h-1.5 rounded-full mt-2 overflow-hidden">
                       <div className="bg-white h-full transition-all duration-1000 ease-linear" style={{ width: `${progress}%` }}></div>
                     </div>
                   )}
                </div>
             </div>
             {stage !== 'COMPLETE' && (
                 <div className="flex justify-between items-center bg-slate-50 p-2 rounded-lg border border-slate-100">
                   <div className="flex items-center space-x-2 overflow-hidden">
                     <div className="w-8 h-8 rounded bg-slate-200 flex-shrink-0">{activeRecipe.imageUrl && <img src={activeRecipe.imageUrl} className="w-full h-full object-cover rounded" />}</div>
                     <div className="min-w-0">
                       <p className="text-[10px] font-bold text-slate-500 uppercase">Current Recipe</p>
                       <p className="text-xs font-bold text-slate-800 truncate">{activeRecipe.name}</p>
                     </div>
                   </div>
                   <button onClick={() => setShowSwitchModal(true)} className="text-[10px] text-blue-600 font-bold hover:underline px-2">Change</button>
                 </div>
             )}
             {stage === 'COMPLETE' && (
               <div className="space-y-3 pt-2 animate-in slide-in-from-bottom-2 bg-slate-50 p-3 rounded-lg border border-slate-200">
                  <h4 className="text-xs font-bold text-slate-500 uppercase mb-2">Quality Check Results</h4>
                  {errorMsg && <div className="p-2 bg-red-100 text-red-700 text-xs rounded mb-2 flex items-center"><AlertTriangle size={12} className="mr-1"/> {errorMsg}</div>}
                  <div className="grid grid-cols-2 gap-3">
                      <div>
                          <label className="text-[10px] font-bold text-nature-700">Good (Kg)</label>
                          <input type="number" className="w-full p-2 text-sm border border-nature-200 bg-nature-50 rounded" placeholder="0.0" value={qcGoodWeight} onChange={e => setQcGoodWeight(e.target.value)} />
                      </div>
                      <div>
                          <label className="text-[10px] font-bold text-red-700">Wastage (Kg)</label>
                          <input type="number" className="w-full p-2 text-sm border border-red-200 bg-red-50 rounded" placeholder="0.0" value={qcWastageWeight} onChange={e => setQcWastageWeight(e.target.value)} />
                      </div>
                  </div>
                  {parseFloat(qcWastageWeight) > 0 && (
                      <div className="space-y-2">
                        <label className="text-[10px] font-bold text-slate-600">Reason for Wastage</label>
                        <select className="w-full p-2 text-xs border rounded" value={wastageReason} onChange={e => setWastageReason(e.target.value)} required>
                            <option value="">Select Reason...</option>
                            <option value="Discoloration">Discoloration</option>
                            <option value="Texture Issue">Texture / Mushy</option>
                            <option value="Contamination">Contamination</option>
                            <option value="Burnt">Burnt / Overcooked</option>
                            <option value="Other">Other</option>
                        </select>
                        {wastageReason === 'Other' && <input className="w-full p-2 text-xs border rounded" placeholder="Please specify reason..." value={customReason} onChange={e => setCustomReason(e.target.value)} />}
                        <div className="text-[10px] text-red-500 font-medium flex items-center"><Trash2 size={12} className="mr-1"/> Cost will be logged to Finance.</div>
                      </div>
                  )}
                  <button onClick={handleFinishBatch} disabled={qcGoodWeight === '' || (parseFloat(qcWastageWeight) > 0 && (!wastageReason || (wastageReason === 'Other' && !customReason)))} className="w-full py-3 bg-earth-800 text-white rounded-lg font-bold hover:bg-earth-900 disabled:opacity-50 shadow-md mt-2">Finish Batch</button>
               </div>
             )}
          </div>
        ) : (
          <div className="py-8 text-center">
            <div className="w-16 h-16 mx-auto bg-slate-50 rounded-full flex items-center justify-center text-slate-300 mb-4"><ChefHat size={32} /></div>
            <button onClick={() => recipes.length === 0 ? alert("Please create a recipe first!") : setShowModal(true)} className="w-full py-3 bg-nature-600 text-white rounded-xl font-bold shadow hover:bg-nature-700 flex items-center justify-center"><Play size={18} className="mr-2" /> Start Processing</button>
          </div>
        )}
      </div>
      <RecipeSelectorModal isOpen={showModal} onClose={() => setShowModal(false)} recipes={recipes} onSelect={(recipe) => { onStart(batch.id, recipe); setShowModal(false); }} title="Start Batch Processing" />
      <RecipeSelectorModal isOpen={showSwitchModal} onClose={() => setShowSwitchModal(false)} recipes={recipes} onSelect={(recipe) => { onSwitchRecipe(batch.id, recipe); setShowSwitchModal(false); }} title="Switch Recipe" />
    </div>
  );
};

const RecipeSelectorModal: React.FC<{
  isOpen: boolean;
  onClose: () => void;
  onSelect: (recipe: Recipe) => void;
  recipes: Recipe[];
  title: string;
}> = ({ isOpen, onClose, onSelect, recipes, title }) => {
  if (!isOpen) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
      <div className="bg-white rounded-2xl shadow-2xl max-w-4xl w-full flex flex-col max-h-[90vh]">
        <div className="p-6 border-b border-slate-100 flex justify-between items-center">
          <h3 className="text-xl font-bold text-slate-900 flex items-center"><ChefHat className="mr-2 text-earth-600" /> {title}</h3>
          <button onClick={onClose} className="p-2 hover:bg-slate-100 rounded-full"><X size={20} className="text-slate-400" /></button>
        </div>
        <div className="p-6 overflow-y-auto grid grid-cols-1 md:grid-cols-2 gap-4">
          {recipes.map((recipe) => (
            <button key={recipe.id} onClick={() => onSelect(recipe)} className="relative text-left rounded-xl border border-slate-200 hover:border-nature-500 hover:ring-1 hover:ring-nature-500 transition-all group overflow-hidden flex h-32">
              <div className="w-32 bg-slate-200 h-full flex-shrink-0 relative">
                {recipe.imageUrl ? <img src={recipe.imageUrl} alt={recipe.name} className="w-full h-full object-cover" /> : <div className="w-full h-full flex items-center justify-center text-slate-400 bg-slate-100"><Utensils size={24} /></div>}
                <div className="absolute top-2 left-2 bg-black/60 text-white text-[10px] font-bold px-1.5 py-0.5 rounded backdrop-blur-sm">{recipe.baseWeightKg}kg Base</div>
              </div>
              <div className="p-4 flex-1 flex flex-col justify-center bg-white group-hover:bg-nature-50">
                <div className="font-bold text-slate-800 mb-1">{recipe.name}</div>
                <div className="flex items-center text-xs text-slate-600 bg-slate-100 px-2 py-1 rounded w-fit"><Clock size={12} className="mr-1 text-slate-400" /> {recipe.cookTimeMinutes} min</div>
              </div>
            </button>
          ))}
          {recipes.length === 0 && <p className="col-span-2 text-center text-slate-400 italic">No recipes available. Go to Recipes tab to create one.</p>}
        </div>
      </div>
    </div>
  );
};

const ProcessingPage: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'floor' | 'recipes'>('floor');
  const [batches, setBatches] = useState<MushroomBatch[]>([]);
  const [loading, setLoading] = useState(true);
  const { recipes, addRecipe, deleteRecipe, updateRecipe, removeDuplicates, isCloud, loading: recipesLoading } = useRecipes();

  const [showRecipeModal, setShowRecipeModal] = useState(false);
  const [editingRecipe, setEditingRecipe] = useState<Partial<Recipe>>({});
  const [recipeImageFile, setRecipeImageFile] = useState<File | null>(null);

  const refreshData = async () => {
    setLoading(true);
    const res = await fetchBatches();
    if (res.success && res.data) {
      setBatches(res.data.filter(b => b.status === BatchStatus.RECEIVED || b.status === BatchStatus.PROCESSING));
    }
    setLoading(false);
  };

  useEffect(() => { refreshData(); }, []);

  const handleStartProcess = async (id: string, recipe: Recipe) => {
    const batch = batches.find(b => b.id === id);
    if (!batch) return;
    const baseWeight = recipe.baseWeightKg || 0.5;
    const ratio = batch.netWeightKg / baseWeight;
    const washTime = Math.ceil(ratio * 60); 
    const drainTime = 120; 
    const cookTime = Math.ceil(ratio * (recipe.cookTimeMinutes * 60)); 
    const config: ProcessConfig = { startTime: Date.now(), washDurationSeconds: washTime, drainDurationSeconds: drainTime, cookDurationSeconds: cookTime, totalDurationSeconds: washTime + drainTime + cookTime };
    const updates = { processConfig: config, selectedRecipeName: recipe.name, recipeType: recipe.name };
    setBatches(prev => prev.map(b => b.id === id ? { ...b, status: BatchStatus.PROCESSING, ...updates } : b));
    await updateBatchStatus(id, BatchStatus.PROCESSING, updates);
  };

  const handleSpeedUp = async (id: string) => {
      const batch = batches.find(b => b.id === id);
      if (!batch || !batch.processConfig) return;
      const finishedTime = Date.now() - (batch.processConfig.totalDurationSeconds * 1000) - 1000;
      const updates = { processConfig: { ...batch.processConfig, startTime: finishedTime } };
      setBatches(prev => prev.map(b => b.id === id ? { ...b, processConfig: updates.processConfig } : b));
      await updateBatchStatus(id, BatchStatus.PROCESSING, updates);
  };

  const handleSwitchRecipe = async (id: string, newRecipe: Recipe) => {
     const batch = batches.find(b => b.id === id);
     if (!batch || !batch.processConfig) return;
     const baseWeight = newRecipe.baseWeightKg || 0.5;
     const ratio = batch.netWeightKg / baseWeight;
     const newCookTime = Math.ceil(ratio * (newRecipe.cookTimeMinutes * 60));
     const newConfig = { ...batch.processConfig, cookDurationSeconds: newCookTime, totalDurationSeconds: batch.processConfig.washDurationSeconds + batch.processConfig.drainDurationSeconds + newCookTime };
     const updates = { processConfig: newConfig, selectedRecipeName: newRecipe.name, recipeType: newRecipe.name };
     setBatches(prev => prev.map(b => b.id === id ? { ...b, ...updates } : b));
     await updateBatchStatus(id, BatchStatus.PROCESSING, updates);
  };

  const handleCompleteProcess = async (id: string, wastage: number, reason?: string) => {
    setBatches(prev => prev.filter(b => b.id !== id));
    await updateBatchStatus(id, BatchStatus.DRYING_COMPLETE, { qualityCheckPassed: true, processingWastageKg: wastage, wastageReason: reason });
  };

  const handleSaveRecipe = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!editingRecipe.name) return;
    // FIX: Defensive check for r.name to prevent toLowerCase error on potentially undefined string
    const isDuplicate = recipes.some(r => 
        (r.name || '').toLowerCase() === (editingRecipe.name || '').toLowerCase() && 
        r.id !== editingRecipe.id 
    );
    if (isDuplicate) { alert("A recipe with this name already exists!"); return; }
    const recipeData = { id: editingRecipe.id || `r-${Date.now()}`, name: editingRecipe.name, type: editingRecipe.type as any || 'CHIPS', baseWeightKg: Number(editingRecipe.baseWeightKg) || 0.5, cookTimeMinutes: Number(editingRecipe.cookTimeMinutes) || 10, temperature: Number(editingRecipe.temperature) || 160, notes: editingRecipe.notes || '', ingredients: editingRecipe.ingredients || '', imageUrl: editingRecipe.imageUrl || '' };
    if (editingRecipe.id) await updateRecipe(recipeData as Recipe, recipeImageFile || undefined);
    else await addRecipe(recipeData as Recipe, recipeImageFile || undefined);
    setRecipeImageFile(null);
    setShowRecipeModal(false);
  };

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      setRecipeImageFile(file);
      const reader = new FileReader();
      reader.onloadend = () => setEditingRecipe({ ...editingRecipe, imageUrl: reader.result as string });
      reader.readAsDataURL(file);
    }
  };

  return (
    <div className="space-y-6 h-full flex flex-col">
      <div className="flex flex-col md:flex-row justify-between items-start md:items-center flex-shrink-0 gap-4 mb-2">
        <h2 className="text-2xl font-bold text-slate-900 flex items-center">
            <Utensils className="mr-3 text-earth-600" size={32} /> Processing Floor
            <div className="ml-4 flex items-center">
                 {isCloud ? (
                     <span className="text-[10px] bg-nature-50 text-nature-700 border border-nature-200 px-2 py-1 rounded-full flex items-center font-bold uppercase tracking-wider">
                        <Cloud size={10} className="mr-1" /> Cloud Sync
                     </span>
                 ) : (
                     <span className="text-[10px] bg-orange-50 text-orange-700 border border-orange-200 px-2 py-1 rounded-full flex items-center font-bold uppercase tracking-wider">
                        <WifiOff size={10} className="mr-1" /> Local Mode
                     </span>
                 )}
            </div>
        </h2>
        <div className="bg-slate-200 p-1 rounded-lg flex text-sm font-medium w-full md:w-auto">
          <button onClick={refreshData} className="px-3 py-2 text-slate-600 hover:text-slate-900 mr-2" title="Force Refresh">
            <RefreshCw size={18} className={loading ? 'animate-spin' : ''} />
          </button>
          <button onClick={() => setActiveTab('floor')} className={`flex-1 md:flex-none px-4 py-2 rounded-md ${activeTab === 'floor' ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-600'}`}>Floor</button>
          <button onClick={() => setActiveTab('recipes')} className={`flex-1 md:flex-none px-4 py-2 rounded-md ${activeTab === 'recipes' ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-600'}`}>Recipes</button>
        </div>
      </div>

      {activeTab === 'floor' && (
        <div className="flex-1 overflow-auto">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6 pb-8">
              {batches.length === 0 && !loading && <div className="col-span-full py-12 text-center text-slate-400"><p>No active batches on the floor.</p></div>}
              {batches.map(batch => (
                <PersistentBatchCard key={batch.id} batch={batch} recipes={recipes} onStart={handleStartProcess} onComplete={handleCompleteProcess} onSwitchRecipe={handleSwitchRecipe} onSpeedUp={handleSpeedUp} />
              ))}
          </div>
        </div>
      )}

      {activeTab === 'recipes' && (
        <div className="flex-1 overflow-auto bg-slate-50 rounded-2xl p-6 border border-slate-100">
          <div className="flex flex-col md:flex-row justify-between items-start md:items-center mb-8 gap-4">
             <div><h3 className="text-xl font-bold text-slate-800 flex items-center"><BookOpen className="mr-2 text-earth-600" />Recipe Library</h3></div>
             <div className="flex items-center space-x-3 w-full md:w-auto">
                <div className="flex space-x-1 bg-white p-1 rounded-lg border border-slate-200 shadow-sm">
                   <button onClick={removeDuplicates} className="p-2 text-slate-500 hover:text-blue-600 hover:bg-blue-50 rounded-md transition-colors" title="Remove Duplicates"><CopyMinus size={18} /></button>
                </div>
                <button onClick={() => { setEditingRecipe({ baseWeightKg: 0.5 }); setRecipeImageFile(null); setShowRecipeModal(true); }} className="flex-1 md:flex-none px-5 py-2.5 bg-nature-600 text-white rounded-lg font-bold shadow-sm hover:bg-nature-700 flex items-center justify-center transition-all">
                    <Plus size={18} className="mr-2" /> New Recipe
                </button>
             </div>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6 pb-8">
             {recipesLoading ? <div className="col-span-full py-20 text-center text-slate-400">Loading recipes...</div> : null}
             {!recipesLoading && recipes.map(recipe => (
               <div key={recipe.id} className="group bg-white rounded-2xl border border-slate-200 shadow-sm hover:shadow-lg transition-all duration-300 overflow-hidden flex flex-col relative">
                  <div className="h-48 relative overflow-hidden bg-slate-100">
                     {recipe.imageUrl ? <img src={recipe.imageUrl} className="w-full h-full object-cover" alt={recipe.name} /> : <div className="w-full h-full flex items-center justify-center text-slate-300 bg-slate-100"><ImageIcon size={40} className="opacity-50" /></div>}
                     <div className="absolute top-3 left-3"><span className="px-2.5 py-1 bg-white/90 backdrop-blur-sm text-slate-700 text-[10px] font-bold uppercase tracking-wider rounded-md border border-slate-100">{recipe.type || 'Standard'}</span></div>
                     <div className="absolute top-3 right-3 opacity-0 group-hover:opacity-100 transition-opacity flex space-x-2">
                        <button onClick={() => { setEditingRecipe(recipe); setRecipeImageFile(null); setShowRecipeModal(true); }} className="p-2 bg-white text-slate-700 rounded-full hover:text-blue-600 shadow-md transition-transform"><Pencil size={16} /></button>
                        <button onClick={() => deleteRecipe(recipe.id)} className="p-2 bg-white text-slate-700 rounded-full hover:text-red-600 shadow-md transition-transform"><Trash2 size={16} /></button>
                     </div>
                  </div>
                  <div className="p-5 flex-1 flex flex-col">
                     <h4 className="font-bold text-lg text-slate-900 mb-1 truncate">{recipe.name}</h4>
                     <div className="flex items-center space-x-3 text-xs text-slate-600 mb-4 mt-2">
                         <div className="flex items-center bg-slate-50 px-2 py-1 rounded border">
                            <Clock size={12} className="mr-1.5 text-slate-400" /> {recipe.cookTimeMinutes}m
                         </div>
                         <div className="flex items-center bg-slate-50 px-2 py-1 rounded border">
                            <Scale size={12} className="mr-1.5 text-slate-400" /> {recipe.baseWeightKg}kg
                         </div>
                     </div>
                     <p className="text-xs text-slate-400 line-clamp-2 mb-4 flex-1">{recipe.notes || "No additional notes."}</p>
                     <div className="mt-auto pt-4 border-t border-slate-50 flex justify-between items-center">
                         <span className="text-[10px] font-bold text-slate-300 uppercase font-mono">#{recipe.id.slice(-4)}</span>
                         <button onClick={() => { setEditingRecipe(recipe); setRecipeImageFile(null); setShowRecipeModal(true); }} className="text-xs font-bold text-slate-400 group-hover:text-nature-600 flex items-center transition-colors">Details <ArrowRight size={12} className="ml-1" /></button>
                     </div>
                  </div>
               </div>
             ))}
             {!recipesLoading && recipes.length === 0 && <div className="col-span-full py-20 text-center border-2 border-dashed border-slate-300 rounded-2xl bg-slate-50/50 flex flex-col items-center justify-center"><h3 className="text-xl font-bold text-slate-700">No Recipes Found</h3><button onClick={() => { setEditingRecipe({ baseWeightKg: 0.5 }); setRecipeImageFile(null); setShowRecipeModal(true); }} className="px-8 py-3 bg-nature-600 text-white rounded-xl font-bold shadow-lg hover:bg-nature-700 mt-8">Create First Recipe</button></div>}
          </div>
        </div>
      )}

      {showRecipeModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
             <div className="bg-white rounded-2xl shadow-xl w-full max-w-lg p-6 max-h-[90vh] overflow-y-auto">
                 <div className="flex justify-between items-center mb-6"><h3 className="text-xl font-bold text-slate-900">{editingRecipe.id ? 'Edit Recipe' : 'New Recipe'}</h3><button onClick={() => setShowRecipeModal(false)} className="p-2 hover:bg-slate-100 rounded-full text-slate-400"><X size={20}/></button></div>
                 <form onSubmit={handleSaveRecipe} className="space-y-5">
                     <div><label className="block text-xs font-bold text-slate-500 mb-1.5 uppercase">Recipe Name</label><input placeholder="e.g. Crispy Oyster Mushrooms" className="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl outline-none" value={editingRecipe.name || ''} onChange={e => setEditingRecipe({...editingRecipe, name: e.target.value})} required /></div>
                     <div className="grid grid-cols-3 gap-4">
                        <div><label className="block text-xs font-bold text-slate-500 mb-1.5 uppercase tracking-wide flex items-center"><Scale size={12} className="mr-1"/> Base Wt (kg)</label><input type="number" step="0.1" className="w-full p-3 bg-slate-50 border rounded-xl outline-none" value={editingRecipe.baseWeightKg || 0.5} onChange={e => setEditingRecipe({...editingRecipe, baseWeightKg: parseFloat(e.target.value)})} /></div>
                        <div><label className="block text-xs font-bold text-slate-500 mb-1.5 uppercase tracking-wide flex items-center"><Clock size={12} className="mr-1"/> Time (min)</label><input type="number" className="w-full p-3 bg-slate-50 border rounded-xl outline-none" value={editingRecipe.cookTimeMinutes || ''} onChange={e => setEditingRecipe({...editingRecipe, cookTimeMinutes: parseFloat(e.target.value)})} /></div>
                        <div><label className="block text-xs font-bold text-slate-500 mb-1.5 uppercase tracking-wide flex items-center"><ThermometerSun size={12} className="mr-1"/> Temp (°C)</label><input type="number" className="w-full p-3 bg-slate-50 border rounded-xl outline-none" value={editingRecipe.temperature || ''} onChange={e => setEditingRecipe({...editingRecipe, temperature: parseFloat(e.target.value)})} /></div>
                     </div>
                     <div><label className="block text-xs font-bold text-slate-500 mb-1.5 uppercase tracking-wide flex items-center"><List size={14} className="mr-1"/> Ingredients</label><textarea placeholder="List ingredients here..." className="w-full p-3 bg-slate-50 border rounded-xl h-24 text-sm outline-none resize-none" value={editingRecipe.ingredients || ''} onChange={e => setEditingRecipe({...editingRecipe, ingredients: e.target.value})} /></div>
                     <div><label className="block text-xs font-bold text-slate-500 mb-1.5 uppercase tracking-wide flex items-center"><FileText size={14} className="mr-1"/> Description</label><textarea placeholder="Process notes..." className="w-full p-3 bg-slate-50 border rounded-xl h-24 text-sm outline-none resize-none" value={editingRecipe.notes || ''} onChange={e => setEditingRecipe({...editingRecipe, notes: e.target.value})} /></div>
                     <label className="flex flex-col items-center justify-center w-full p-4 border-2 border-dashed border-slate-300 rounded-xl cursor-pointer hover:border-nature-500 hover:bg-nature-50 text-slate-500 relative overflow-hidden h-40 transition-all">
                        {editingRecipe.imageUrl ? <div className="absolute inset-0 w-full h-full"><img src={editingRecipe.imageUrl} className="w-full h-full object-cover opacity-50" /><div className="absolute inset-0 flex items-center justify-center"><div className="bg-white/90 backdrop-blur-sm px-4 py-2 rounded-full shadow-sm text-xs font-bold text-slate-700">Change Photo</div></div></div> : <><div className="bg-slate-100 p-3 rounded-full mb-2"><Upload size={24} className="text-slate-400" /></div><span className="text-xs font-bold">Click to Upload Photo</span></>}
                        <input type="file" className="hidden" accept="image/*" onChange={handleFileUpload} />
                     </label>
                     <div className="flex space-x-3 pt-2">
                        <button type="button" onClick={() => setShowRecipeModal(false)} className="flex-1 py-3 text-slate-500 hover:bg-slate-50 rounded-xl font-bold">Cancel</button>
                        <button type="submit" className="flex-1 py-3 bg-nature-600 text-white rounded-xl font-bold hover:bg-nature-700 shadow-lg">Save Recipe</button>
                     </div>
                 </form>
             </div>
          </div>
      )}
    </div>
  );
};

export default ProcessingPage;