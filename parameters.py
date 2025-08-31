import os
from google.cloud import bigquery
from google import genai

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/mariana/.config/gcloud/application_default_credentials.json"

# os.environ['project_id'] = "mlops-project-430120" 
# os.environ['staging_dataset_id'] = "STAGING_DATA"
# os.environ['marts_dataset_id'] = "MARTS_DATA"
# os.environ['connection_id'] = 'my-connection'
# os.environ['genai_api_key'] = None
project_id = os.environ['project_id']
staging_dataset_id = os.environ['staging_dataset_id']
marts_dataset_id = os.environ['marts_dataset_id']
connection_id = os.environ['connection_id']

news_content_table_id = f"{project_id}.{staging_dataset_id}.news_content_usa"
drivers_data_table_id = f"{project_id}.{staging_dataset_id}.drivers_data"
rides_data_table_id = f"{project_id}.{staging_dataset_id}.rides_data"
articles_metrics_table_id = f"{project_id}.{marts_dataset_id}.articles_metrics"
drivers_metrics_table_id = f"{project_id}.{marts_dataset_id}.drivers_metrics"
drivers_reason_tags_table_id = f"{project_id}.{marts_dataset_id}.drivers_reason_tags"
driver_reason_embeddings_table_id = f"{project_id}.{marts_dataset_id}.driver_reason_embeddings"

client = bigquery.Client(project=project_id)
genai_api_key = os.environ['genai_api_key']
genai_client = genai.Client(api_key=genai_api_key,
    vertexai=True, project=project_id, location="us-central1"
) 

news_content_usa_schema = [
    bigquery.SchemaField("article_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("published", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("content", "STRING", mode="REQUIRED"),
]

drivers_data_schema = [
    bigquery.SchemaField("Driver_ID", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Age", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("City", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Experience_Years", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Average_Rating", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Active_Status", "STRING", mode="NULLABLE"),
]

rides_data_schema = [
    bigquery.SchemaField("Ride_ID", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Driver_ID", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("City", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("Distance_km", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Duration_min", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Fare", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Rating", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Promo_Code", "STRING", mode="NULLABLE"),
]

embedding_schema = [
    bigquery.SchemaField("driver_ID", "INT64"),
    bigquery.SchemaField("stress_report_tags", "STRING"),
    bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
]

usa_articles = [
{
  "article_name": "In Chicago, a Coalition of Unions, Community Organizers, and Drivers Have Forced Uber to Come to the Table",
  "city": "Chicago",
  "published": "2025-07-16",
  "content": "Chicago—Last month, a coalition of rideshare drivers, grassroots organizations, and unions announced an agreement with Uber declaring that the company would support Illinois state legislation enabling drivers to unionize and then bargain around pay and working conditions in the rideshare industry. This agreement was a result of Chicago drivers’ organizing for more than six years—a story that shows both the potential and the challenges of worker organizing in the tech industry as that industry takes a sharp turn towards the far right.\n\nWhen Uber and Lyft were founded, drivers and passengers alike were excited by the technology and its promise of low prices for passengers and decent pay and flexible work schedules for drivers. For a time, these advantages overshadowed an industry business model that relied on a workforce of independent contractors with no worker protections and a company culture that prioritized growth at all costs. Over the past decade, it has become clear that this early honeymoon period was only temporary and had been subsidized by venture capital to undercut competition and corner the market. Beginning in 2017, both companies gradually decreased pay for drivers, who went from making over a dollar a mile in 2015 to $.64 a mile in 2022—even as the cost of vehicle ownership and maintenance skyrocketed and the cost the companies charged passengers increased by 83 percent from the beginning of 2018 to the third quarter of 2022. Although at one point Lyft had a reputation as a friendlier, more socially responsible company, drivers have consistently experienced broad similarities in the pay and working conditions between the two companies. Today, neither company provides any kind of standard time or distance-based pay rate to drivers in Chicago, and the cost of every ride as well as how much of the total fare the driver receives are determined in an unpredictable way by a mysterious algorithm. What has become clear is that these app companies rely on worker exploitation and paying workers below the minimum wage in order to be profitable.\n\nDrivers in Chicago started organizing in 2017, directly following Ubers announcement that workers would no longer receive the bulk of their “surge” pay (the upcharge passengers pay when the platforms are busy). Even then, the writing was on the wall, as Uber described itself explicitly as a “technology” company rather than a transportation company—and both companies have continually tried to divest from the workforce they begrudgingly rely on until the time they hope to replace drivers with self-driving vehicles. Driver organizing began through an informal self-organized group and then, starting in 2019, became a project of The People’s Lobby called Chicago Gig Alliance. Drivers lobbied at City Hall and held rallies, protests, acts of civil disobedience, and vigils for workers killed on the job. We shared harrowing stories of assaults on the job, arbitrary firings by algorithm, and pay rates that left drivers in poverty and even sometimes homeless.\n\nIn 2022, Chicago Gig Alliance worked with allies in the Chicago City Council to draft and introduce an ordinance to raise pay and improve working conditions. Drivers held meetings with their council members to ask them to support this ordinance and held rallies and direct actions to call attention to the dramatically worsening pay and conditions in the industry—a campaign which earned dozens of media hits that reached millions of Chicagoans.\n\nThe People’s Lobby included support for that ordinance as a condition of the organization’s 2023 endorsement process, which led then–mayoral candidate Brandon Johnson and a dozen members of the City Council to commit to supporting the rideshare living-wage ordinance before drivers and other People’s Lobby members knocked thousands of doors to help them win their elections. In response to ongoing driver organizing as well as the progressive momentum coming from the 2023 election, Alderman Mike Rodriguez reintroduced the ordinance in the first meeting of the new council in May 2023. Over the next 18 months, drivers and Alderman Rodriguez built the cosponsor list up to 29 alders.\n\nThroughout this process, Chicago drivers were in constant communication with drivers in other cities fighting similar campaigns. PowerSwitch Action and Action Center on Race and the Economy brought together driver organizations from a number of states to learn from successes and failures and provide research and legal support. Drivers from places like California, Colorado, and Seattle were soon traveling to Chicago to participate in rallies for our rideshare living-wage ordinance, and drivers from Chicago marched on Uber"
},
{
  "article_name": "At Los Angeles International Airport, drivers spend hours in a “holding pen” waiting to be matched with passengers.",
  "city": "Los Angeles",
  "published": "2025-05-14",
  "content": "Before the sun could rise over Los Angeles International Airport on a recent Tuesday, hundreds of Uber and Lyft drivers had formed a queue nearby, stretching around the block. It was 5 a.m., and the waiting game was about to begin. In a few minutes, the line of cars would file into a fenced-off parking lot, a mile from the arrival terminals. It is known officially as the Transportation Network Company Staging Area, but drivers call it the “pen,” where they wait to be matched with passengers getting off flights. The spot used to be a prime place to catch rides and earn decent money. But these days, there seem to be few rides to go around. Veronica Hernandez, 50, parked her white Chevy Malibu at 5:26 a.m. and opened the Lyft app to check her place in the queue: 156th. It would be an hour and a half before her first ride of the day.\n\n“You have good days and bad days,” Ms. Hernandez said, swiping through a screen showing her daily earnings on the app that week: $205, $245, $179. “Hopefully it’s a good day.” Like ride-hailing drivers across the country, Ms. Hernandez has seen her pay decline in recent years, even as the demand for her work feels greater than ever. And with the cost of gas and car insurance rising, the already slim margins of gig work are becoming less workable by the day, she said. No place is more emblematic of these problems than LAX, one of the busiest airports in the world but one of the most difficult places for gig workers to earn a living. “It used to be a real way to earn money,” Ms. Hernandez said. “Now you can barely survive on it.”\n\nIn the early years of app-based platforms like Uber, Lyft and DoorDash, people flocked to sign up as drivers. The idea of making money simply by driving someone around in your own car, on your own schedule, appealed to many. The key concept was that drivers would be independent contractors, responsible for their own expenses, without health insurance or other employee benefits but with the flexibility to work whatever hours they wanted. And in the early years, wages were high. Drivers would regularly take home thousands of dollars a week, as Uber and Lyft pushed growth over profits. Then, when they became public companies, profitability became a focus, and wages gradually shrank.\n\nNow, earnings have fallen behind inflation, and for many drivers have decreased. Last year, Uber drivers made an average of $513 a week in gross earnings, a 3.4 percent decline from the previous year, even as they worked six minutes more a week on average, according to Gridwise. For drivers in Los Angeles, average hourly earnings on Uber are down 21 percent since 2021, Gridwise found.\n\nLAX introduced the new system in 2019, in an effort to cut down on bumper-to-bumper traffic at the arrival terminal. Passengers must walk or take a shuttle from their terminal to a pickup spot called LAX-it. But the driver side is rarely seen. Inside the lot, with hundreds of parked cars and the smell of port-a-potties, the mood was grim. Drivers waited for hours to snare rides — “unicorns,” they called them — that would pay them more than $1.50 per mile.\n\nBy 10 a.m., the pen had devolved into chaos. While around 300 drivers are waiting in the virtual queue, the lot has only around 200 spots. Cars double-park, honk, yell — all drowned by the roar of jet planes. Sergio Avedian, a senior contributor to a ride-hailing blog, settled into the pen and saw $9.87 for a 13-mile trip and $19.97 for a 25-mile trip. He rejected them all: “We call this ‘decline and recline.’”\n\nDrivers nap, smoke, sell items, pray, argue. A separate economy feeds them with Chinese food and taco trucks. Some scribble frustrations inside port-a-potties. Andreh Andrias, 57, from Iran, used to make $3,000 a week. Now it's half that, but with $7,000 in expenses. “Right now, I cannot,” he said.\n\nUber said rising insurance costs and LAX surcharges hurt demand. Lyft said they are working with LAX on improvements. In the lot, fatigue rules. “You’re just chasing that ride, that score,” said Pablo Gomez. When the pen closes at 2 a.m., some drivers go sleep in their cars until it reopens. Ms. Hernandez ended her day at 11 p.m. with a $28 ride toward home. “It’s not the best rate,” she said. “But you have to make it worth your time.”"
},
{
  "article_name": "Uber sues Florida law firm, medical clinics for fraud over car crashes",
  "city": "Miami",
  "published": "2025-06-11",
  "content": "June 11 (Reuters) - Ride-hailing company Uber filed a lawsuit against a Miami law firm and a Miami medical center accusing them of orchestrating a medical and insurance scam that involved staged car collisions. Uber, in the lawsuit filed on Tuesday in federal court in Miami, accuses personal injury firm Law Group of South Florida and firm attorney Andy Loynaz of participating in a scheme to pay drivers to intentionally collide with other cars and then claim they were using the Uber app while driving.\n\nAuto mechanics and healthcare providers at River Medical Center in Miami and other clinics falsely said the crashes caused severe injuries and required medical treatments, according to the lawsuit. Those false statements formed the basis of bogus lawsuits against Uber and its insurer.\n\nLoynaz, described on the law firm's website as a co-founder, declined to comment. A person who answered the phone at River Medical Center declined to comment and declined to make anyone else available.\n\nThe lawsuit claims that four fraudulent lawsuits were filed in Florida court stemming from the scheme, but it didn’t specify how much they cost Uber. However, Uber said it has spent several million dollars in defense costs and settlements.\n\nAdam Blinick, Uber's head of state and local public policy in the U.S. and Canada, stated that Uber will take action if it sees something inappropriate on the platform.\n\nThe 97-page lawsuit details five allegedly staged crashes, all of which took place near Hialeah, Florida, in 2023 and 2024. After each crash, Loynaz submitted insurance claims for the maximum amount allowed under the policy: $1 million. Following four of the crashes, lawsuits were filed naming Uber and its commercial auto liability insurance carrier. Some of these lawsuits are still pending in court."
},
{
  "article_name": "Bill enabling unionization of ride-hail drivers takes big step",
  "city": "San Francisco",
  "published": "2025-06-12",
  "content": "The state Assembly this month overwhelmingly passed a bill that would permit such workers to organize. The legislation — Assembly Bill 1340 — is now in the state Senate, where it will likely be taken up by one or two committees before receiving a floor vote. The last day for each chamber to pass bills is Sept. 12."
},
{
  "article_name": "NYC proposes 5 percent raise for rideshare drivers in a bid to appease Uber and Lyft",
  "city": "New York",
  "published": "2025-06-20",
  "content": "New York City's Taxi and Limousine Commission (TLC) have settled on new minimum-wage rules for rideshare drivers, Bloomberg reports. Drivers will receive a five percent raise under the new proposal, a compromise to keep Uber and Lyft from locking drivers out of their apps. The proposal needs to be voted on by the TLC's board of commissioners before it goes into effect, but assuming it does, it'll end months of uncertainty for drivers working in the city. Uber began sporadically locking drivers out of its app in May 2024, preventing them from taking rides and earning money. The company was blocking access to its app to avoid having to pay drivers who were working but not actively taking rides. Besides introducing a minimum wage for drivers that started around $18 per hour in 2022, New York also included stipulations in its law that required drivers be paid for the downtime between rides, something Uber and Lyft naturally had a problem with. Bloomberg writes that the TLC initially proposed a 6.1 percent raise in an attempt to disincentivize Uber and Lyft from locking drivers out. The proposal would adjust how driver pay is calculated, in exchange for an upfront raise and a guarantee that drivers are warned before they lose access to a rideshare app. Settling on a five percent raise and a commitment to not raise wages yearly and instead base it on 'changing industry dynamics,' is a further capitulation. One that's still not enough for Lyft, apparently. The company told Bloomberg that, 'while these changes are a step in the right direction, we still have concerns that the underlying pay formula will still deprive drivers of earning opportunities, drive up prices for riders and reduce ride availability.' Uber and Lyft have long had a contentious relationship with city and state governments over driver protections. In comparison to the passing of Prop 22 in California, which reclassified gig workers as contractors after another law did the opposite, even a diminished minimum wage law in New York is better than nothing."
}
]