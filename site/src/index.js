import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import Filter from './Filter';
import * as serviceWorker from './serviceWorker';

ReactDOM.render(
  <React.StrictMode>
    <Filter />
  </React.StrictMode>,
  document.getElementById('filter')
);

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA
serviceWorker.unregister();
