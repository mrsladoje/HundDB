import { useState, useEffect } from "react";
import { FaShield, FaX } from "react-icons/fa6";
import { FaHome, FaPaw } from "react-icons/fa";
import { MdMenu } from "react-icons/md";
import { FiSettings } from "react-icons/fi";
import NavButton from "@/components/navbar/NavButton.jsx";

export const Navbar = () => {
  const [isMenuOpen, setIsMenuOpen] = useState(false);
  const [activeTab, setActiveTab] = useState("home");

  useEffect(() => {
    const handleResize = () => {
      if (window.innerWidth >= 768 && isMenuOpen) {
        setIsMenuOpen(false);
      }
    };

    window.addEventListener('resize', handleResize);
    
    return () => window.removeEventListener('resize', handleResize);
  }, [isMenuOpen]);

  const navItems = [
    {
      id: "home",
      label: "Home",
      icon: FaHome,
      description: "PUT, GET, DELETE operations",
    },
    {
      id: "config",
      label: "Config",
      icon: FiSettings,
      description: "System settings & parameters",
    },
    {
      id: "integrity",
      label: "Data",
      icon: FaShield,
      description: "Merkle tree validation",
    },
  ];

  const handleTabClick = (tabId) => {
    setActiveTab(tabId);
    setIsMenuOpen(false);
  };

  return (
    <nav className="bg-sloth-yellow border-b-2 border-sloth-brown shadow-[0_4px_0px_0px_#6b5e4a] absolute top-0 left-0 w-full z-50 select-none">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 relative overflow-hidden">
        <FaPaw className="absolute top-2.5 left-[50%] md:left-[22%] opacity-30 text-sloth-brown -rotate-12 text-4xl" />
        <FaPaw className="absolute top-1 right-[15%] opacity-20 text-sloth-brown rotate-30 text-2xl" />
        <FaPaw className="absolute bottom-1.5 -left-[3%] opacity-25 text-sloth-brown -rotate-12 text-3xl" />
        <FaPaw className="absolute top-[20%] -right-[10%] opacity-15 text-sloth-brown rotate-6 text-xl" />
        <FaPaw className="absolute bottom-2 right-[25%] opacity-30 text-sloth-brown -rotate-30 text-2xl" />
        <FaPaw className="absolute top-[40%] left-[5%] opacity-20 text-sloth-brown rotate-12 text-lg" />
        <FaPaw className="absolute bottom-[50%] left-[70%] opacity-25 text-sloth-brown rotate-75 text-2xl" />
        <FaPaw className="absolute top-[70%] right-[30%] md:right-[80%] opacity-15 text-sloth-brown -rotate-30 text-xl" />
        <div className="flex justify-between items-center h-16 md:h-[4.5rem] lg:h-[5.9rem]">
          {/* Mobile Layout */}
          <div className="md:hidden flex items-center justify-between w-full">
            {/* Logo for mobile */}
            <div className="flex items-center group">
              <img
                src="./pics/rokica.png"
                alt="HundDB Logo"
                className="w-full h-12 object-contain hover:scale-110 hover:-rotate-3 active:scale-100 active:rotate-0 transition-transform duration-300"
                onError={(e) => {
                  e.target.style.display = "none";
                  e.target.nextSibling.style.display = "flex";
                }}
              />
              <h1 className="text-2xl text-sloth-brown-dark font-bold tracking-wide ml-2 relative">
                HundDB
                <div className="absolute h-0.5 w-0 group-hover:w-full bg-sloth-brown transition-all duration-[250ms] ease-in" />
              </h1>
            </div>

            <MobileMenuButton
              isOpen={isMenuOpen}
              onClick={() => setIsMenuOpen(!isMenuOpen)}
            />
          </div>

          {/* Desktop Layout - Left Navigation */}
          <div className="hidden md:flex items-center">
            <NavButton
              icon={FaHome}
              label="Home"
              isActive={activeTab === "home"}
              onClick={() => handleTabClick("home")}
            />
          </div>

          {/* Desktop Layout - Center Logo */}
          <div className="hidden md:flex items-center justify-center group">
            <div className="relative">
              <img
                src="./pics/rokica.png"
                alt="HundDB Logo"
                className="w-full h-[3.75rem] lg:h-[4.425rem] object-contain hover:scale-105 hover:-rotate-3 active:scale-100 active:rotate-0 transition-transform duration-200"
                onError={(e) => {
                  e.target.style.display = "none";
                  e.target.nextSibling.style.display = "flex";
                }}
              />
            </div>
            <h1 className="text-sloth-brown-dark font-extrabold tracking-wider text-3xl lg:text-4xl ml-2 lg:ml-2.5 relative">
              HundDB
              <div className="absolute h-1 w-0 group-hover:w-full bg-sloth-brown transition-all duration-[250ms] ease-in" />
            </h1>
          </div>

          {/* Desktop Layout - Right Navigation */}
          <div className="hidden md:flex items-center gap-4">
            <NavButton
              icon={FiSettings}
              label="Config"
              isActive={activeTab === "config"}
              onClick={() => handleTabClick("config")}
            />
            <NavButton
              icon={FaShield}
              label="Data"
              isActive={activeTab === "integrity"}
              onClick={() => handleTabClick("integrity")}
            />
          </div>
        </div>

        
      </div>

      {/* Mobile Menu */}
        <MobileMenu
          isOpen={isMenuOpen}
          navItems={navItems}
          activeTab={activeTab}
          onTabClick={handleTabClick}
        />

      {!isMenuOpen && (
        <div className="h-[0.05rem] bg-sloth-brown border-t-2 border-sloth-brown-dark" />
      )}
    </nav>
  );
};

const MobileMenuButton = ({ isOpen, onClick }) => {
  return (
    <button
      onClick={onClick}
      className="md:hidden p-[0.4rem] bg-sloth-brown text-sloth-yellow border-[3px] border-sloth-brown-dark shadow-[4px_4px_0px_0px_#000000] active:translate-y-[4px] active:translate-x-[4px] active:shadow-none transition-all duration-200"
    >
      {isOpen ? <FaX className="w-4 h-4" /> : <MdMenu className="w-5 h-5" />}
    </button>
  );
};

// Mobile Menu Component
const MobileMenu = ({ isOpen, navItems, activeTab, onTabClick }) => {
  if (!isOpen) return null;

  return (
    <div className="md:hidden absolute top-full left-0 w-full bg-sloth-yellow z-40">
      <div className="p-4 !pt-3 space-y-3 relative overflow-hidden">
        <FaPaw className="absolute top-2.5 left-[50%] md:left-[22%] opacity-30 text-sloth-brown -rotate-12 text-4xl" />
        <FaPaw className="absolute top-1 right-[15%] opacity-20 text-sloth-brown rotate-30 text-2xl" />
        <FaPaw className="absolute bottom-1.5 -left-[3%] opacity-25 text-sloth-brown -rotate-12 text-3xl" />
        <FaPaw className="absolute top-[20%] -right-[10%] opacity-15 text-sloth-brown rotate-6 text-xl" />
        <FaPaw className="absolute bottom-2 right-[25%] opacity-30 text-sloth-brown -rotate-30 text-2xl" />
        <FaPaw className="absolute top-[40%] left-[5%] opacity-20 text-sloth-brown rotate-12 text-lg" />
        <FaPaw className="absolute bottom-[50%] left-[70%] opacity-25 text-sloth-brown rotate-75 text-2xl" />
        <FaPaw className="absolute top-[70%] right-[30%] md:right-[80%] opacity-15 text-sloth-brown -rotate-30 text-xl" />
        {navItems.map((item, index) => {
          const Icon = item.icon;
          const isActive = activeTab === item.id;

          return (
            <button
              key={item.id}
              onClick={() => onTabClick(item.id)}
              className={`
                w-full p-4 text-left border-4 border-black font-black text-lg
                transform transition-all duration-200
                ${
                  isActive
                    ? "bg-sloth-brown text-sloth-yellow shadow-[4px_4px_0px_0px_#000000] translate-x-1"
                    : "bg-sloth-yellow-lite text-sloth-brown hover:bg-sloth-yellow shadow-[3px_3px_0px_0px_#000000] hover:shadow-[4px_4px_0px_0px_#000000] hover:translate-x-1 active:translate-y-1"
                }
                active:translate-x-2 active:shadow-[2px_2px_0px_0px_#000000]
              `}
              style={{
                animationDelay: `${index * 100}ms`,
              }}
            >
              <div className="flex items-center gap-3">
                <Icon className="w-6 h-6" />
                <div>
                  <div>{item.label}</div>
                  <div className="text-sm font-medium opacity-70 mt-1">
                    {item.description}
                  </div>
                </div>
              </div>
            </button>
          );
        })}
      </div>
      <div className="h-2 bg-sloth-brown border-t-2 border-black" />
    </div>
  );
};

export default Navbar;
