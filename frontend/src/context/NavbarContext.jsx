import { createContext, useContext, useState } from "react";

export const NavbarContext = createContext(null);

export const NavbarProvider = ({ children }) => {
  const [navbarHeight, setNavbarHeight] = useState(null);

  return (
    <NavbarContext.Provider
      value={{
        navbarHeight,
        setNavbarHeight,
      }}
    >
      {children}
    </NavbarContext.Provider>
  );
};

export const useNavbar = () => {
  const context = useContext(NavbarContext);
  if (!context) {
    throw new Error("useNavbar must be used within a NavbarProvider");
  }
  return context;
};

export default NavbarProvider;