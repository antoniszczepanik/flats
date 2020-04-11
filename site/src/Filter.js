import React, { Component } from 'react';
import './Filter.css';

//Todo: Something is no yes - cannot read state in fuctnion
//Should use controlled components...
class Filter extends Component {
  state = {
    size_min: 10,
    size_max: 40,
    transaction: 'wynająć',
    city: 'Warszawie',
  };

//  renderTransaction = () => {
//    return {state.transaction};
//  }
//  rendeSize = () => {
//    return {state.size_min};
//  }
//  renderCity = () => {
//    return {state.city};
//  }

  render() {
    return (
      <div>
        Chcę 
//        {this.renderTransaction()}
        mieszkanie wielkości
 //       {this.renderSize()} m2 w 
  //      {this.renderCity()}.
      </div>
    );
  };
}

export default Filter;
