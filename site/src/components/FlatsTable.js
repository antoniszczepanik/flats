import React, { Component } from 'react';

import FlatRow from './FlatRow.js';
import './FlatsTable.css';



class FlatsTable extends Component {
  render() {
   let offer_type = this.props.transaction;
   let filtered_flats = this.props.flats.filter( function(flat) {
       return flat.offer_type === offer_type;
   });

   return (
      <div>
        <table className="flatsTable">
        {
            filtered_flats.map((flat, index) => (
                <FlatRow key={index} flat={flat}/>
            ))
        }
        </table>

     </div>

      );
  };
}


export default FlatsTable;
