import { Link } from 'react-router-dom'
import { ReactNode } from 'react'
import logo from '../../assets/logo.svg'

export default function PatchNotesPublicShell({ children }: { children: ReactNode }) {
  return (
    <div className="h-screen bg-neutral-950 text-neutral-100 flex flex-col overflow-hidden">
      <header className="shrink-0 border-b border-neutral-800 bg-neutral-900/80 backdrop-blur-xl">
        <div className="max-w-4xl mx-auto px-6 py-4 flex items-center justify-between gap-4">
          <Link to="/patch-notes" className="flex items-center gap-3">
            <img src={logo} alt="" className="w-10 h-10 object-contain opacity-90" />
            <span className="font-black text-white tracking-tight">Patch Notes</span>
          </Link>
          <Link
            to="/"
            className="text-xs font-semibold text-blue-400 hover:text-blue-300 uppercase tracking-wider"
          >
            Portal login
          </Link>
        </div>
      </header>
      <main className="flex-1 flex flex-col min-h-0 overflow-hidden w-full">{children}</main>
    </div>
  )
}
