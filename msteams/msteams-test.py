import pymsteams

# You must create the connectorcard object with the Microsoft Webhook URL
myTeamsMessage = pymsteams.connectorcard("https://outlook.office.com/webhook/bb759118-475a-4db8-bdf0-aa101b66564b@29abf16e-95a2-4d13-8d51-6db1b775d45b/IncomingWebhook/373cbd3700b44495a47f11c5a1c82a85/748e4c17-20cb-40e4-9aee-c3c16ef7f1d3")

myTeamsMessage.title("ODS-HRIS-WebService running report")
# Add text to the message.
myTeamsMessage.text("esb job trigger by 2020-02-06 5:00:00")

# Create Section 1
Section1 = pymsteams.cardsection()
Section1.text("WorkExperienceInfo report")
# Facts are key value pairs displayed in a list.
Section1.addFact("expected total.","sync total.")
Section1.addFact("10000","10000")

Section2 = pymsteams.cardsection()
Section2.text("TerminationHistory report")
# Facts are key value pairs displayed in a list.
Section2.addFact("expected total.","sync total.")
Section2.addFact( "10000","10000")
Section3 = pymsteams.cardsection()
Section3.text("WorkExperienceInfo report")
# Facts are key value pairs displayed in a list.
Section3.addFact("expected total.","sync total.")
Section3.addFact("10000","10000")
Section4 = pymsteams.cardsection()
Section4.text("TakeHomePay report")
# Facts are key value pairs displayed in a list.
Section4.addFact("expected total.","sync total.")
Section4.addFact( "10000","10000")

# Add both Sections to the main card object
myTeamsMessage.addSection(Section1)
myTeamsMessage.addSection(Section2)
myTeamsMessage.addSection(Section3)
myTeamsMessage.addSection(Section4)



# send the message.
myTeamsMessage.send()

# Preview your object
# myTeamsMessage.printme()
