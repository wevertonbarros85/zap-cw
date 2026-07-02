import React, { createContext, useState } from "react";
import useWhatsApps from "../../hooks/useWhatsApps";

const WhatsAppsContext = createContext();

const WhatsAppsProvider = ({ children }) => {
  // Add fallback values to prevent destructuring errors
  const whatsAppData = useWhatsApps();
  const { loading = false, whatsApps = [] } = whatsAppData || {};
  const [error] = useState(null);

  // Log error state for debugging
  if (error) {
    console.warn("WhatsAppsProvider error:", error);
  }

  return (
    <WhatsAppsContext.Provider value={{ whatsApps, loading, error }}>
      {children}
    </WhatsAppsContext.Provider>
  );
};

export { WhatsAppsContext, WhatsAppsProvider };