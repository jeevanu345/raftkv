import { useRef, useState } from 'react';
import { ArrowRight, Check, Database, GitBranch, Layers, Moon, Sun } from 'lucide-react';
import './welcome.css';

function ClusterIllustration() {
  return (
    <div className="welcome-cluster" aria-label="Illustration of a leader replicating data to two follower nodes" role="img">
      <div className="welcome-cluster-caption"><GitBranch size={14} /><span>One log. A shared state.</span></div>
      <div className="welcome-cluster-map">
        <svg className="welcome-cluster-links" viewBox="0 0 480 194" preserveAspectRatio="none" aria-hidden="true">
          <path d="M240 65 V110 Q240 121 228 121 H100 Q88 121 88 134 V151" />
          <path d="M240 65 V110 Q240 121 252 121 H380 Q392 121 392 134 V151" />
        </svg>
        <div className="welcome-node welcome-node-leader"><Database size={20} /><span><strong>Node 01</strong><small>Leader</small></span><span className="welcome-node-dot" /></div>
        <div className="welcome-replication"><Check size={12} /> Replicate &amp; commit</div>
        <div className="welcome-node welcome-node-follower welcome-node-left"><Database size={17} /><span><strong>Node 02</strong><small>Follower</small></span></div>
        <div className="welcome-node welcome-node-follower welcome-node-right"><Database size={17} /><span><strong>Node 03</strong><small>Follower</small></span></div>
      </div>
      <div className="welcome-cluster-footer"><span><i /> Consensus, made visible</span><span>Raft protocol</span></div>
    </div>
  );
}

export default function Welcome({ onContinue, theme, onToggleTheme }) {
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [error, setError] = useState('');
  const [errorField, setErrorField] = useState('');
  const nameRef = useRef(null);
  const emailRef = useRef(null);

  function enterWorkspace(event) {
    event.preventDefault();
    const profile = { name: name.trim(), email: email.trim() };
    if (!profile.name || /[\u0000-\u001f\u007f]/.test(profile.name)) {
      setError('Enter your name to personalize this workspace.');
      setErrorField('name');
      nameRef.current?.focus();
      return;
    }
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(profile.email)) {
      setError('Enter a valid email address, such as alex@example.com.');
      setErrorField('email');
      emailRef.current?.focus();
      return;
    }
    setError('');
    onContinue(profile);
  }

  return (
    <div className="welcome-shell">
      <a className="skip-link" href="#welcome-main">Skip to content</a>
      <header className="welcome-header">
        <a className="welcome-brand" href="#welcome-main" aria-label="RaftKV Console home"><img src="/mark.svg" alt="" width={36} height={36} /><span>RaftKV <small>Console</small></span></a>
        <button type="button" className="welcome-theme" onClick={onToggleTheme} aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}>
          {theme === 'dark' ? <Sun size={17} /> : <Moon size={17} />}<span>{theme === 'dark' ? 'Light' : 'Dark'} theme</span>
        </button>
      </header>
      <main className="welcome-main" id="welcome-main" tabIndex={-1} aria-labelledby="welcome-title">
        <section className="welcome-intro" aria-labelledby="welcome-title">
          <h1 id="welcome-title">Your cluster.<br />A clearer view.</h1>
          <p className="welcome-description">Understand consensus, explore your data, and manage your RaftKV cluster from one focused workspace.</p>
          <ClusterIllustration />
          <div className="welcome-capabilities">
            <div><GitBranch size={18} /><span><strong>Follow consensus</strong><small>Inspect nodes, terms, and replication.</small></span></div>
            <div><Layers size={18} /><span><strong>Work with your data</strong><small>Browse keys and trace the replicated log.</small></span></div>
          </div>
        </section>
        <section className="welcome-signin" aria-labelledby="signin-title">
          <h2 id="signin-title">Make yourself at home.</h2>
          <p>Set up your local profile to enter the console.</p>
          <form onSubmit={enterWorkspace} className="welcome-form">
            <div className="welcome-field"><label htmlFor="operator-name">Your name</label><input ref={nameRef} id="operator-name" name="name" autoComplete="name" placeholder="Alex Morgan" value={name} onChange={event => { setName(event.target.value); setError(''); setErrorField(''); }} required maxLength={80} aria-invalid={errorField === 'name' || undefined} aria-describedby={error && errorField === 'name' ? 'welcome-form-error' : undefined} /></div>
            <div className="welcome-field"><label htmlFor="operator-email">Email address</label><input ref={emailRef} id="operator-email" name="email" type="email" autoComplete="email" autoCapitalize="none" spellCheck={false} placeholder="alex@example.com" value={email} onChange={event => { setEmail(event.target.value); setError(''); setErrorField(''); }} required maxLength={254} aria-invalid={errorField === 'email' || undefined} aria-describedby={error && errorField === 'email' ? 'welcome-form-error' : undefined} /></div>
            {error && <p id="welcome-form-error" role="alert" className="welcome-form-error">{error}</p>}
            <button type="submit" className="welcome-submit">Enter workspace <ArrowRight size={17} /></button>
          </form>
          <div className="welcome-profile-note"><span className="welcome-note-mark" aria-hidden="true">i</span><p>Local workspace profile. Node APIs are not authenticated.<small>Your profile stays in this browser tab. No password is required.</small></p></div>
        </section>
      </main>
      <footer className="welcome-footer"><span>RaftKV Console</span><span>Built for understanding distributed systems.</span></footer>
    </div>
  );
}
