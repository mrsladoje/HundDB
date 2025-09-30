import "./App.css";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import Navbar from "@/components/navbar/Navbar.jsx";
import Home from "@/pages/Home.jsx";
import Config from "@/pages/Config.jsx";
import Data from "@/pages/Data.jsx";
import Probabilistic from "@/pages/Probabilistic.jsx";
import NavbarProvider from "@/context/NavbarContext";

function App() {
  return (
    <Router>
      <NavbarProvider>
        <div className="w-full min-h-screen">
          <Navbar />
          <main>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/config" element={<Config />} />
              <Route path="/data" element={<Data />} />
              <Route path="/probabilistic" element={<Probabilistic />} />
            </Routes>
          </main>
        </div>
      </NavbarProvider>
    </Router>
  );
}

export default App;
