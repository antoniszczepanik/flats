import React, { Component } from 'react';
import './SearchBar.css';


class SearchBar extends Component {

  render() {
    return (
      <div className="filter">
        Chcę 

        <select className="transaction" value={this.props.transaction} onChange={this.props.handleTransactionChange}>
          <option value="rent">wynajać</option>
          <option value="sale">kupić</option>   
        </select>

        mieszkanie za

        <nobr>
          <input className="min_price" type="text" value={this.props.min_price} onChange={this.props.handleMinPriceChange} />
          -
          <input className="max_price" type="text" value={this.props.max_price} onChange={this.props.handleMaxPriceChange} />
          {this.formatCurrency()}zł w
        </nobr>

        <select className="city" value={this.props.city} onChange={this.props.handleCityChange}>
          <option value="warszawa">Warszawie</option>   
          <option value="krakow">Krakowie</option>
          <option value="gdansk">Gdańsku</option>
        </select>

      </div>

    );
  };

  formatCurrency() {
    let transaction = this.props.transaction;
    return transaction === 'sale' ? 'tysięcy ' : '';
  };

}

export default SearchBar;
