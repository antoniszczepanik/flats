import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import FilterableFlatTable from './components/FilterableFlatTable';
import * as serviceWorker from './serviceWorker';

const FLATS = [
    {
      'url': '<a href=https://www.morizon.pl/oferta/sprzedaz-mieszkanie-lodz-86m2-mzn2036166499" target="_blank">Link</a>',
      'added':'2020-04-17',
      'title':'Kraków fajne mieszkanie',
      'size':86.0,
      'price':550.000,
      'estimate':1524700,
      'offer_type':'sale'
   
    },
    {
      'url': '<a href=https://www.morizon.pl/oferta/sprzedaz-mieszkanie-lodz-86m2-mzn2036166499" target="_blank">Link</a>',
      'added':'2020-04-17',
      'title':'Kraków ekstra mieszkanie',
      'size':86.0,
      'price':550.000,
      'estimate':1524700,
      'offer_type':'sale'
   
    },
    {
      'url': '<a href=https://www.morizon.pl/oferta/sprzedaz-mieszkanie-lodz-86m2-mzn2036166499" target="_blank">Link</a>',
      'added':'2020-04-17',
      'title':'Kraków średnie mieszkanie',
      'size':86.0,
      'price':550.000,
      'estimate':1524700,
      'offer_type':'sale'
   
    },
    {
      'url': '<a href=https://www.morizon.pl/oferta/sprzedaz-mieszkanie-lodz-86m2-mzn2036166499" target="_blank">Link</a>',
      'added':'2020-04-17',
      'title':'Kraków słabe mieszkanie',
      'size':86.0,
      'price':550.000,
      'estimate':1524700,
      'offer_type':'rent'
   
    },
    {
      'url': '<a href=https://www.morizon.pl/oferta/sprzedaz-mieszkanie-lodz-86m2-mzn2036166499" target="_blank">Link</a>',
      'added':'2020-04-17',
      'title':'Kraków inne mieszkanie',
      'size':86.0,
      'price':550.000,
      'estimate':1524700,
      'offer_type':'rent'
   
    }
]

ReactDOM.render(
  <React.StrictMode>
    <FilterableFlatTable flats={FLATS} />
  </React.StrictMode>,
  document.getElementById('root'),
);

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA
serviceWorker.unregister();
