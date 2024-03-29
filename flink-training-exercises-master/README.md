
## RideCleanisingExercise

```javascript
public class RideCleansingExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		DataStream<TaxiRide> filteredRides = rides
				// filter out rides that do not start or stop in NYC
				.filter(new NYCFilter());

		// print the filtered stream
		printOrTest(filteredRides);

		// run the cleansing pipeline
		env.execute("Taxi Ride Cleansing");
	}

	private static class NYCFilter implements FilterFunction<TaxiRide> {

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {
			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}

}
![alt text](https://sun9-21.userapi.com/impg/XIEYWBRAObFwXUIDJqyBbo43__39FVhL-G3-cA/-sYyQ3u9bvo.jpg?size=1790x974&quality=96&sign=81244dd5941ecef6eb58674c9bebc6f4&type=album)
```
## RidesAndFaresExercise


