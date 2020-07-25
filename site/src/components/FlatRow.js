import React, { Component } from 'react';
import './FlatRow.css';


class FlatRow extends Component {
  
  formatPrice(number, transaction) {
      return (transaction === 'sale' ? Math.round(number/1000)*1000 : Math.round(number/100)*100);
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
                <p className="flatRowDescription"> 
                    Warte ok.<span></span>
                    <span className="flatRowPrice"> {this.formatPrice(this.props.flat.estimate, this.props.transaction)} </span>
                    zł, wystawione za <span></span>
                    <span className="flatRowPrice"> {this.props.flat.price} </span>
                    zł <span></span>
                </p>
            </td>
        </tr>
      </div>

    );
  };
}

export default FlatRow;
