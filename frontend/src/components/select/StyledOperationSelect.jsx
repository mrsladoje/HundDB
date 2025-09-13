import Select from 'react-select';

// Custom styles for react-select to match neobrutalistic design
const customSelectStyles = {
  control: (provided, state) => ({
    ...provided,
    border: '4px solid #4b4436', // sloth-brown-dark
    borderRadius: '8px',
    minHeight: '52px', // Match the height of other inputs
    backgroundColor: 'white',
    boxShadow: state.isFocused 
      ? '2px 2px 0px 0px rgba(139,119,95,1)' 
      : '3px 3px 0px 0px rgba(139,119,95,1)',
    transform: state.isFocused ? 'translate(1px, 1px)' : 'none',
    transition: 'all 0.2s',
    cursor: 'pointer',
    '&:hover': {
      border: '4px solid #4b4436',
    }
  }),
  valueContainer: (provided) => ({
    ...provided,
    padding: '0 16px',
    fontWeight: '500',
    color: '#4b4436', // sloth-brown-dark
  }),
  singleValue: (provided) => ({
    ...provided,
    color: '#4b4436', // sloth-brown-dark
    fontWeight: '500',
  }),
  placeholder: (provided) => ({
    ...provided,
    color: '#4b4436',
    fontWeight: '500',
    opacity: 0.7,
  }),
  indicatorSeparator: () => ({
    display: 'none',
  }),
  dropdownIndicator: (provided, state) => ({
    ...provided,
    color: '#4b4436',
    transform: state.selectProps.menuIsOpen ? 'rotate(180deg)' : 'none',
    transition: 'transform 0.2s',
    '&:hover': {
      color: '#6b5e4a', // darker brown
    }
  }),
  menu: (provided) => ({
    ...provided,
    border: '4px solid #4b4436',
    borderRadius: '8px',
    boxShadow: '6px 6px 0px 0px rgba(107,94,74,1)',
    backgroundColor: 'white',
    overflow: 'hidden',
    marginTop: '4px',
  }),
  menuList: (provided) => ({
    ...provided,
    padding: 0,
    maxHeight: '250px',
    // Custom scrollbar styles
    '&::-webkit-scrollbar': {
      width: '12px',
    },
    '&::-webkit-scrollbar-track': {
      backgroundColor: '#fcefd7', // sloth-yellow-lite
      borderRadius: '0px',
    },
    '&::-webkit-scrollbar-thumb': {
      backgroundColor: '#6b5e4a', // sloth-brown
      borderRadius: '0px',
      border: '2px solid #fcefd7', // sloth-yellow-lite border
    },
    '&::-webkit-scrollbar-thumb:hover': {
      backgroundColor: '#4b4436', // sloth-brown-dark on hover
    },
    // Firefox scrollbar styles
    scrollbarWidth: 'thin',
    scrollbarColor: '#6b5e4a #fcefd7', // thumb color, track color
    scrollBehavior: 'smooth',
  }),
  option: (provided, state) => ({
    ...provided,
    backgroundColor: state.isSelected 
      ? '#6b5e4a' // sloth-brown
      : state.isFocused 
        ? '#fcefd7' // sloth-yellow-lite
        : 'white',
    color: state.isSelected 
      ? '#fcefd7' // sloth-yellow-lite
      : '#4b4436', // sloth-brown-dark
    fontWeight: '500',
    padding: '12px 16px',
    cursor: 'pointer',
    borderBottom: state.isLast ? 'none' : '2px solid #fcefd7',
    '&:active': {
      backgroundColor: state.isSelected ? '#6b5e4a' : '#edd6ab', // sloth-yellow
    }
  }),
};

// The options array for your operations
const operationOptions = [
  { value: 'GET', label: 'GET - Fetch Record' },
  { value: 'PUT', label: 'PUT - Save Record' },
  { value: 'DELETE', label: 'DELETE - Remove Record' },
  { value: 'PREFIX_SCAN', label: 'PREFIX SCAN - Find by Prefix' },
  { value: 'RANGE_SCAN', label: 'RANGE SCAN - Find by Range' },
  { value: 'PREFIX_ITERATE', label: 'PREFIX ITERATE - Iterate by Prefix' },
  { value: 'RANGE_ITERATE', label: 'RANGE ITERATE - Iterate by Range' },
];

// The styled Select component
const StyledOperationSelect = ({ value, onChange, isDisabled }) => {
  return (
    <Select
      value={operationOptions.find(option => option.value === value)}
      onChange={(selectedOption) => onChange(selectedOption.value)}
      options={operationOptions}
      styles={customSelectStyles}
      isDisabled={isDisabled}
      isSearchable={false}
      placeholder="Choose your mission..."
      className="react-select-container"
      classNamePrefix="react-select"
      menuPortalTarget={document.body}
    />
  );
};

export default StyledOperationSelect;