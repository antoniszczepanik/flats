import React, { Component } from 'react';
import './FlatRow.css';

class FlatRow extends Component {

  render() {
    return (
      <div> 
        {this.props.flat.title}
      </div>

    );
  };
}

export default FlatRow;
