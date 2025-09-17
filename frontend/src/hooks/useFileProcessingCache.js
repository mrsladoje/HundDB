import { useRef, useCallback } from 'react';

// LRU Cache implementation
class LRUCache {
  constructor(maxSize = 5) {
    this.maxSize = maxSize;
    this.cache = new Map();
  }

  get(key) {
    if (this.cache.has(key)) {
      // Move to end (most recently used)
      const value = this.cache.get(key);
      this.cache.delete(key);
      this.cache.set(key, value);
      return value;
    }
    return undefined;
  }

  set(key, value) {
    if (this.cache.has(key)) {
      // Update existing
      this.cache.delete(key);
    } else if (this.cache.size >= this.maxSize) {
      // Remove least recently used (first item)
      const firstKey = this.cache.keys().next().value;
      const firstValue = this.cache.get(firstKey);
      
      // Clean up object URLs if they exist
      if (firstValue && firstValue.objectUrl) {
        URL.revokeObjectURL(firstValue.objectUrl);
      }
      
      this.cache.delete(firstKey);
    }
    
    this.cache.set(key, value);
  }

  clear() {
    // Clean up all object URLs before clearing
    for (const [key, value] of this.cache) {
      if (value && value.objectUrl) {
        URL.revokeObjectURL(value.objectUrl);
      }
    }
    this.cache.clear();
  }

  size() {
    return this.cache.size;
  }
}

// Custom hook for file processing with LRU cache
export const useFileProcessingCache = (maxCacheSize = 5) => {
  const cacheRef = useRef(new LRUCache(maxCacheSize));

  const processFile = useCallback((fileType, actualValue, recordId) => {
    if (!fileType || !actualValue) return null;

    const cacheKey = `${recordId}-${fileType}`;
    
    // Check cache first
    const cached = cacheRef.current.get(cacheKey);
    if (cached) {
      return cached;
    }

    // Process the file
    const byteArray = actualValue.split(",").map((x) => {
      const num = parseInt(x.trim());
      return isNaN(num) ? 0 : num;
    });

    const uint8Array = new Uint8Array(byteArray);
    let objectUrl = null;

    // Create blob and object URL for previewable types
    if (['mp3', 'wav', 'ogg', 'png', 'jpg', 'jpeg', 'gif', 'pdf'].includes(fileType)) {
      const mimeType = {
        pdf: "application/pdf",
        png: "image/png",
        jpg: "image/jpeg",
        jpeg: "image/jpeg",
        gif: "image/gif",
        mp3: "audio/mpeg",
        wav: "audio/wav",
        ogg: "audio/ogg",
      }[fileType] || "application/octet-stream";

      const blob = new Blob([uint8Array], { type: mimeType });
      objectUrl = URL.createObjectURL(blob);
    }

    const result = {
      uint8Array,
      objectUrl,
      fileType,
      processedAt: Date.now()
    };

    // Store in cache
    cacheRef.current.set(cacheKey, result);

    return result;
  }, []);

  const clearCache = useCallback(() => {
    cacheRef.current.clear();
  }, []);

  const getCacheSize = useCallback(() => {
    return cacheRef.current.size();
  }, []);

  // Cleanup on unmount
  const cleanupCache = useCallback(() => {
    cacheRef.current.clear();
  }, []);

  return {
    processFile,
    clearCache,
    getCacheSize,
    cleanupCache
  };
};

// Alternative: Simple memoization with size limit for text content
export const useTextCache = (maxSize = 10) => {
  const cacheRef = useRef(new Map());

  const memoizedProcess = useCallback((key, processingFn) => {
    if (cacheRef.current.has(key)) {
      return cacheRef.current.get(key);
    }

    const result = processingFn();

    // Simple size management - remove oldest if over limit
    if (cacheRef.current.size >= maxSize) {
      const firstKey = cacheRef.current.keys().next().value;
      cacheRef.current.delete(firstKey);
    }

    cacheRef.current.set(key, result);
    return result;
  }, [maxSize]);

  return memoizedProcess;
};