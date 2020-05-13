import React, { Component } from 'react';
import FilteredResultsTable from './FilteredResultsTable.js';
import './Filter.css';
import RentData from '../data/rent.json';


class Filter extends Component {

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
      <div className="filter">
        Chcę 

        <select className="transaction" value={this.state.transaction} onChange={this.handleTransactionChange}>
          <option value="rent">wynajać</option>
          <option value="sale">kupić</option>   
        </select>

        mieszkanie za

        <nobr>
          <input className="min_price" type="text" value={this.state.min_price} onChange={this.handleMinPriceChange} />
          -
          <input className="max_price" type="text" value={this.state.max_price} onChange={this.handleMaxPriceChange} />
          {this.formatCurrency()}zł w
        </nobr>

        <select className="city" value={this.state.city} onChange={this.handleCityChange}>
          <option value="Warszawa">Warszawie</option>   
          <option value="Kraków">Krakowie</option>
          <option value="Gdańsk">Gdańsku</option>
        </select>

        <FilteredResultsTable
          min_price={this.state.min_price}
          max_price={this.state.max_price}
          city={this.state.city}
          transaction={this.state.transaction}
          data={RentData}

        />

      </div>

    );
  };

  formatCurrency() {
    const { transaction } = this.state;
    return transaction === 'sale' ? 'tysięcy ' : '';
  };

}

export default Filter;
