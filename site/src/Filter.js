import React, { Component } from 'react';
import './Filter.css';

//Todo: Something is no yes - cannot read state in fuctnion
//Should use controlled components...
class Filter extends Component {
  constructor(props) {
    super(props);
    this.state = {
      min_size: 40,
      max_size: 50,
      transaction: 'sale',
      city: 'Warszawie',
    };
    this.handleMinSizeChange = this.handleMinSizeChange.bind(this);
    this.handleMaxSizeChange = this.handleMaxSizeChange.bind(this);
    this.handleTransactionChange = this.handleTransactionChange.bind(this);
    this.handleCityChange = this.handleCityChange.bind(this);
  }

  handleTransactionChange(event) {
    this.setState({transaction: event.target.value});
  }
  handleMaxSizeChange(event) {
    this.setState({max_size: event.target.value});
  }
  handleMinSizeChange(event) {
    this.setState({min_size: event.target.value});
  }
  handleCityChange(event) {
    this.setState({city: event.target.value});
  }

  render() {
    return (
      <div id="filter">
        Chcę 
        <select id="transaction" value={this.state.transaction} onChange={this.handleTransactionChange}>
          <option value="rent">wynajać</option>
          <option value="sale">kupić</option>   
        </select>
        mieszkanie wielkości
        <input id="min_size" type="text" value={this.state.min_size} onChange={this.handleMinSizeChange} />
        -
        <input id="max_size" type="text" value={this.state.max_size} onChange={this.handleMaxSizeChange} />
        m2 w
        <input id="city" type="text" value={this.state.city} onChange={this.handleCityChange} />
      </div>

    );
  };
}

export default Filter;