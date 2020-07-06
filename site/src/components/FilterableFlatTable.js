import React, { Component } from 'react';
import FlatsTable from './FlatsTable.js';
import SearchBar from './SearchBar.js';
import './FilterableFlatTable.css';

class FilterableFlatTable extends Component {

  constructor(props) {
    super(props);
    this.state = {
      min_price: 400,
      max_price: 500,
      transaction: 'sale',
      city: 'Warszawa',
    };
    this.handleMinPriceChange = this.handleMinPriceChange.bind(this);
    this.handleMaxPriceChange = this.handleMaxPriceChange.bind(this);
    this.handleTransactionChange = this.handleTransactionChange.bind(this);
    this.handleCityChange = this.handleCityChange.bind(this);
  }

  handleTransactionChange(event) {
    this.setState(
      {
      transaction: event.target.value,
      min_price: event.target.value === 'sale' ? 400 : 2000,
      max_price: event.target.value === 'sale' ? 600 : 3000,
      }
    );
  }

  handleMaxPriceChange(event) {
    this.setState({max_price: event.target.value});
  }

  handleMinPriceChange(event) {
    this.setState({min_price: event.target.value});
  }

  handleCityChange(event) {
    this.setState({city: event.target.value});
  }

  render() {
    return (
      <div> 
        <SearchBar
          transaction={this.state.transaction}
          handleTransactionChange={this.handleTransactionChange}

          min_price={this.state.min_price}
          handleMinPriceChange={this.handleMinPriceChange}

          max_price={this.state.max_price}
          handleMaxPriceChange={this.handleMaxPriceChange}

          city={this.state.city}
          handleCityChange={this.handleCityChange}
        />
        <br/>
        <FlatsTable 
          flats={this.props.flats}
          transaction={this.state.transaction}
          min_price={this.state.min_price}
          max_price={this.state.max_price}
          city={this.state.city}
        />
      </div>
    );
  };
}

export default FilterableFlatTable;
