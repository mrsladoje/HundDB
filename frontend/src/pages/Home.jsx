import React, { useState } from 'react';
import { Get, Put } from '@wails/main/App.js'; 

export const Home = () => {
  const [key, setKey] = useState('');
  const [value, setValue] = useState('');
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  const handleGet = async () => {
    setError(null);
    setResult(null);
    try {
      const record = await Get(key);
      if (record) {
        setResult(`Found record: ${JSON.stringify(record, null, 2)}`);
      } else {
        setResult('Record not found.');
      }
    } catch (err) {
      setError(`Error getting key: ${err}`);
    }
  };

  const handlePut = async () => {
    setError(null);
    setResult(null);
    try {
      // The `Put` method in your Go code takes a `string` and a `[]byte`.
      // JavaScript strings are UTF-8, so you can pass a string directly and Wails
      // will handle the conversion.
      await Put(key, value);
      setResult(`Successfully put key: ${key}`);
    } catch (err) {
      setError(`Error putting key: ${err}`);
    }
  };

  return (
    <div>
      <h1>HundDB LSM Tree</h1>
      <div className="input-group">
        <input
          type="text"
          placeholder="Key"
          value={key}
          onChange={(e) => setKey(e.target.value)}
        />
        <input
          type="text"
          placeholder="Value"
          value={value}
          onChange={(e) => setValue(e.target.value)}
        />
        <button onClick={handleGet}>Get</button>
        <button onClick={handlePut}>Put</button>
      </div>
      {result && <pre style={{ color: 'lime' }}>{result}</pre>}
      {error && <pre style={{ color: 'red' }}>{error}</pre>}
    </div>
  );
}

export default Home;