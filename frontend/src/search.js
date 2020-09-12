import React from 'react';

class Search extends React.Component {

  editTypeTerm = (e) => {
    this.props.editTypeTerm(event.target.value)
  }

  editLocationTerm = (e) => {
    this.props.editLocationTerm(event.target.value)
  }

  submitSearch = () => {
    this.props.submitSearch()
  }

  dynamicSearch = () => {
    return this.state.names.filter(name => name.toLowerCase().includes(this.state.searchTerm.toLowerCase()))
  }

    render(){
      return (
        <div className="search-bar">
          {/* <input type= 'text' value = {this.props.typeTerm} onChange = {this.editTypeTerm} placeholder = 'Search by type'/> */}
          <input type= 'text' value = {this.props.locationTerm} onChange = {this.editLocationTerm} placeholder = 'Search by address'/> 
          <button onClick = {this.submitSearch}>Submit Search</button>        
        </div>
      );
    }
}

export default Search;