import React, { Component } from 'react';
import FlatsTable from './FlatsTable.js';
import SearchBar from './SearchBar.js';
import './FilterableFlatTable.css';

class FilterableFlatTable extends Component {

  render() {
    return (
      <div> 
        <p> FilterableFlatTable </p>
        <SearchBar/>
        <br/>
        <FlatsTable/>
      </div>
    );
  };
}

export default FilterableFlatTable;
