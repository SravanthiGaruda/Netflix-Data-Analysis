# Netflix-Data-Analysis
This project processes Netflix movie rating data and title data using Apache Hadoop's MapReduce framework. It performs a join operation between ratings and titles, calculates the average rating for each movie, and outputs the results.

### Project Structure

- **Rating**: A Writable class that represents a movie rating with a movie ID and rating value.
- **Title**: A Writable class that represents a movie title with a movie ID, year, and title name.
- **RatingTitle**: A Writable class used for the join operation, it can hold either a Rating or a Title.
- **Result**: A Writable class that holds the final output with the average rating and title.
- **Netflix**: The main class that contains the MapReduce job setup and execution.

### Classes and Methods

#### `Rating`
- **Fields**: `int movieID`, `int rating`
- **Methods**: `write`, `readFields`

#### `Title`
- **Fields**: `int movieID`, `String year`, `String title`
- **Methods**: `write`, `readFields`

#### `RatingTitle`
- **Fields**: `short tag`, `Rating rating`, `Title title`
- **Methods**: `write`, `readFields`

#### `Result`
- **Fields**: `float rating`, `String title`
- **Methods**: `write`, `readFields`, `toString`

#### `Netflix`
- Contains the main MapReduce job setup and execution.
- Three main classes:
  - `RatingMapper`
  - `TitleMapper`
  - `ResultReducer`

### Input and Output

#### Input
- Ratings file: CSV file with fields `[userID, movieID, rating, timestamp]`
- Titles file: CSV file with fields `[movieID, year, title]`

#### Output
- Text file with the average rating and title for each movie in the format: `year: title average_rating`

### MapReduce Flow

1. **RatingMapper**:
   - Reads the ratings file and emits `(movieID, Rating)`

2. **TitleMapper**:
   - Reads the titles file and emits `(movieID, Title)`

3. **ResultReducer**:
   - Joins ratings and titles by `movieID`
   - Computes the average rating for each movie
   - Emits the result in the format `(Text, Result)`
