import './App.css'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import Navbar from '@/components/navbar/Navbar.jsx'
import Home from '@/pages/Home.jsx'
import Config from '@/pages/Config.jsx'
import Data from '@/pages/Data.jsx'

function App() {

  return (
    <Router>
      <div className="App">
        <Navbar />
        <main>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/config" element={<Config />} />
            <Route path="/data" element={<Data />} />
          </Routes>
        </main>
      </div>
    </Router>
  )
}

export default App
