import React, { Component } from 'react';

import FlatRow from './FlatRow.js';
import './FlatsTable.css';



class FlatsTable extends Component {
  render() {
   let offer_type = this.props.transaction;
   let min_price = this.props.min_price;
   let max_price = this.props.max_price;
   let city = this.props.city;
   if (offer_type === 'sale') {
       // Convert sale limits to thousands
       min_price *= 1000
       max_price *= 1000
   }
   let filtered_flats = this.props.flats.filter(flat => flat.offer_type === offer_type)
                                        .filter(flat => flat.price >= min_price)
                                        .filter(flat => flat.price >= min_price)
                                        .filter(flat => flat.price <= max_price)
                                        .filter(flat => flat.city === city);
   return (
      <div>
        <table className="flatsTable">
        {
            filtered_flats.map((flat, index) => (
                <FlatRow 
                    key={index} 
                    flat={flat}
                    transaction={this.props.transaction}
                />
            ))
        }
        </table>

     </div>

      );
  };
}


export default FlatsTable;
