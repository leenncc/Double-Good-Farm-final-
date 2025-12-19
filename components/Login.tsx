
import React, { useState } from 'react';
import { auth, db } from '../services/firebase';
import { Leaf } from 'lucide-react';

export default function Login({ onLogin }: { onLogin: () => void }) {
  const [isSignUp, setIsSignUp] = useState(false);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    
    try {
      if (isSignUp) {
        const cred = await auth.createUserWithEmailAndPassword(email, password);
        // Default role is GUEST. Admin must change this in Firebase Console.
        if (cred.user) {
            await db.collection('user_roles').doc(cred.user.uid).set({
                role: 'GUEST', 
                email: email
            });
        }
        alert("Account created! Please ask Admin to assign your role (PROCESSING, SALES, or ADMIN).");
      } else {
        await auth.signInWithEmailAndPassword(email, password);
      }
      onLogin(); 
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-slate-100 p-4">
      <div className="bg-white p-8 rounded-2xl shadow-xl w-full max-w-md border border-slate-200">
        <div className="flex flex-col items-center mb-8">
            <div className="p-3 bg-nature-100 rounded-full text-nature-600 mb-3">
                <Leaf size={32} />
            </div>
            <h1 className="text-2xl font-bold text-slate-800">Double Good Farming</h1>
            <p className="text-slate-500">{isSignUp ? 'Create Staff Account' : 'Staff Login'}</p>
        </div>

        {error && <div className="bg-red-50 text-red-600 p-3 rounded-lg mb-4 text-sm font-medium flex items-center"><span className="mr-2">⚠️</span> {error}</div>}
        
        <form onSubmit={handleSubmit} className="space-y-4">
            <div>
                <label className="block text-xs font-bold text-slate-500 uppercase mb-1">Email</label>
                <input 
                    type="email" 
                    placeholder="name@company.com" 
                    className="w-full p-3 border border-slate-300 rounded-lg focus:ring-2 focus:ring-nature-500 outline-none transition-all" 
                    value={email} 
                    onChange={e => setEmail(e.target.value)} 
                    required 
                />
            </div>
            <div>
                <label className="block text-xs font-bold text-slate-500 uppercase mb-1">Password</label>
                <input 
                    type="password" 
                    placeholder="••••••••" 
                    className="w-full p-3 border border-slate-300 rounded-lg focus:ring-2 focus:ring-nature-500 outline-none transition-all" 
                    value={password} 
                    onChange={e => setPassword(e.target.value)} 
                    required 
                />
            </div>
            
            <button disabled={loading} className="w-full bg-earth-800 text-white p-3 rounded-lg font-bold hover:bg-earth-900 transition-colors shadow-lg disabled:opacity-50">
                {loading ? 'Processing...' : (isSignUp ? 'Create Account' : 'Log In')}
            </button>
        </form>

        <div className="mt-6 text-center border-t border-slate-100 pt-4">
            <p className="text-sm text-slate-500 cursor-pointer hover:text-earth-700 font-medium transition-colors" onClick={() => setIsSignUp(!isSignUp)}>
                {isSignUp ? 'Already have an account? Log In' : 'New Staff? Create Account'}
            </p>
        </div>
      </div>
    </div>
  );
}