from datetime import datetime
dates = ['01 Apr 2017', '01 Apr 2018', '01 Aug 2017', '01 Aug 2018', '01 Dec 2017', '01 Dec 2018', '01 Feb 2017', '01 Feb 2018', '01 Jan 2017', '01 Jan 2018']
dates.sort(key=lambda date:datetime.strptime(date,'%d %b %Y'))
print(dates)

