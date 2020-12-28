import React, { Component } from 'react';
import './FlatRow.css';


class FlatRow extends Component {
  
  getSalePrices(flat){
      let estimate = Math.round(flat.estimate/1000)*1000;
      return (
        <p className="flatRowDescription"> 
            Warte ok.<span></span>
            <span className="flatRowPrice"> {estimate.toLocaleString('en-US').replace(',', ' ')} </span>
            zł, wystawione za <span></span>
            <span className="flatRowPrice"> {flat.price.toLocaleString('en-US').replace(',', ' ')} </span>
            zł <span></span>
        </p>
      )
  }
  getRentPrices(flat){
      let estimate = Math.round(flat.estimate/100)*100;
      return (
        <p className="flatRowDescription"> 
            Warte ok.<span></span>
            <span className="flatRowPrice"> {estimate} </span>
            zł, wystawione za <span></span>
            <span className="flatRowPrice"> {flat.price} </span>
            zł <span></span>
        </p>
      )
  }

  render() {
    return (
      <div>
        <hr className="flatRowRuler"></hr>
        <tr className="flatRowRow"> 
            <td className="flatRowSize">
                    <nobr>{this.props.flat.size}㎡</nobr>
                    <nobr><p className="flat_added"> {this.props.flat.added} </p> </nobr>
            </td>
            <td>
                <p className="flatRowTitle">
                    <a href={this.props.flat.url}> {this.props.flat.title} </a>
                </p>
                {this.props.transaction === 'sale' ? this.getSalePrices(this.props.flat) : this.getRentPrices(this.props.flat)}
            </td>
        </tr>
      </div>

    );
  };
}

export default FlatRow;
