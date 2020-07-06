import React, { Component } from 'react';
import './FlatRow.css';


class FlatRow extends Component {
  
  getRandomEmoji() {
    var emoji_array = [
        '🤯', '💎', '😬', '🤔'
    ];
    let emoji = emoji_array[Math.floor(Math.random()*emoji_array.length)];
    return emoji
  };

  render() {
    return (
      <div>
        <hr className="flatRowRuler"></hr>
        <tr className="flatRowRow"> 
            <td className="flatRowSize"> <nobr>{this.props.flat.size}㎡</nobr></td>
            <td>
                <p className="flatRowTitle">
                    {this.props.flat.title}
                </p>
                <p className="flatRowDescription"> 
                    Warte około <span></span>
                    <span className="flatRowPrice"> {this.props.flat.estimate} </span>
                    zł, wystawione za <span></span>
                    <span className="flatRowPrice"> {this.props.flat.price} </span>
                    zł <span></span>
                    {this.getRandomEmoji()}
                </p>
            </td>
        </tr>
      </div>

    );
  };
}

export default FlatRow;
