import React, { Component } from 'react';

import FlatRow from './FlatRow.js';
import './FlatsTable.css';



class FlatsTable extends Component {
  render() {
   return (
      <div>
        <p> Flats Table </p>
        <div>
        {
            this.props.flats.map((flat, index) => (
                <FlatRow key={index} flat={flat}/>
            ))
        }
        </div>
     </div>

      );
  };
}


export default FlatsTable;
