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
            <td className="flatRowSize">
                    <nobr>{this.props.flat.size}㎡</nobr>
                    <p className="flat_added"> {this.props.flat.added} </p>
            </td>
            <td>
                <p className="flatRowTitle">
                    <a href={this.props.flat.url}> {this.props.flat.title} </a>
                </p>
                <p className="flatRowDescription"> 
                    Warte ok.<span></span>
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
