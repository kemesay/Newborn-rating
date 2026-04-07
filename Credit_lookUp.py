#lookUp Table
lookup_table = {

########################## Security Related Factors ##########################

    "Type of collateral":{
        "Cash, Cash Substitutes": (20, 10),
        "Building, Share certificate, Merchandize": (18, 10),
        "Machinery, Vehicle": (16, 20),
        "Corporate/Government/Bank and Personal Guarantee": (12, 20),
        "Export Document": (14, 30),
        "Land Lease Right/Coffee Plantation": (12, 30),
        "Second Degree Mortgage": (10, 25),

    },
   
   ### For Normal or personal loan
    "Collateral to Loan Coverage Ratio": [
        (29, 0, 0),
        (30, 30, 10),
        (40, 40, 10),
        (50, 50, 10),  
        (60, 60, 20),
        (80, 70, 30),
        (100, 75, 20),
        (110, 80, 10),
    ],

    ### For Cooperatives
    "Collateral to Loan Coverage Ratio": [
        (9, 0, 0),
        (10, 30, 0),
        (20, 40, 0),
        (30, 50, 10),
        (40, 60, 10),
        (50, 70, 10),  
        (60, 80, 20),
    ],

    ########################## Management Capacity(DDR) ##########################

    "Experience in related Line of Business(Business Age)": [
        (2, 1, 10),
        (3, 2, 40),
        (7, 3, 15),
        (10, 4, 5),
         ],

   "Qualification(Education Level)":{
       
        "Degree and above": (3, 15),
        "Diploma": (2, 40),
        "High School": (1, 10),
        "Other": (0, 30),
    },
    
    "Availability of clear duty and Segregation": {
        "Clear outlined duties and responsibility supported by organizational structure": (2, 10),
        "Semi segregated duties and responsibilities supported by organizational structure": (1, 40),
        "Unclear duties, responsibilities and control system breached": (0, 30),
    },

########################## Integrity and Transparency(DDR) ##########################

    "Consistency of presented document": {
        "Adequate": (2, 10),
        "Acceptable": (1, 40),
        "Inconsistent": (0, 30),
    },

    "Responsiveness to wards bank's inquiry": {
        "Provides consistently prompt": (2, 10),
        "Response and willingly": (1.5, 40),
        "Responds with repeated Inquiry": (1, 30),
        "Not responsive or willing": (0, 20),
    },

    "Corporate Responsibility (Tax Payment)": {
        "Perfect": (2, 10),
        "Acceptable": (1, 40),
        "Inconsistent": (0, 30),
    },


    ########################## Banking Relationship  with CBO (CBS) ##########################

    "Length of relationship with CBO(account age)": [
        (0, 0, 0),
        (0.5, 0.5, 0),
        (1, 1, 20),
        (2, 2, 20),
        (3, 4, 30),
    ],

    "The account transaction to sales percentage share with CBO": [
        (50, 1, 0),
        (60, 2, 10),
        (70, 3, 20),
        (80, 4, 30),
        (90, 5, 20),
        (100, 6, 10),
    ],

    ########################### Credit History(25%) ##########################

          ######################### Swing(Loan Stmnt) #########################

    "Highest debit": [
      
        (60, 3, 30),
        (84, 5, 20),
        (85, 6, 10),
    ],

    "Lowest debit ": {
        "At least credit balance within three months ": (7, 10),
        "At least a credit balance within six months ": (6, 40),
        "At least a credit balance within a year": (5, 30),
        "At least 4% debit balance within a year": (3, 20),
        "Greater than 4% debit balance within a year": (0, 10),
    },
        
    "SwingTurnover": [
        (0, 0, 0),
        (1, 4, 0),
        (2.5, 7, 20),
        (4, 8, 20),
    ],

          ######################### T/Loan Performance (CIC) #########################

    "Existing loan repayment": {

        "Regular repayment": (13, 10),
        "1 - 30 days in arrears": (10, 40),
        "31-90 days in arrears": (5, 30),
        "More than 90 days in arrears": (0, 20),
    },

    "Settled loans": {
        "Settled with regular repayment": (8, 10),
        "Settled timely but with an element of irregularity": (7, 40),
        "Settled before sixty days after due date ": (5, 30),
        "Settled after 60 days but less than ninety days": (3, 20),
        "Settled  after  being NPL and/  or  through foreclosure, legal action": (0, 10),
    },

          ######################### L/C Facility (Loan Stmnt) #########################

    "Average settlement of import L/C facility": {
        "Settled within a month": (21, 10),
        "Settled within two months": (15, 40),
        "Settled within three months": (5, 30),
        "Settled after three months": (0, 20),
    },

          ######################### Merchandise Loan Limit(Loan Stmnt) #########################

    "Settlement of Merchandise Loan": {
        "Settled all advances within due date": (13, 10),
        "Settled within 30 days from the due date": (10, 40),
        "Settled within 45 days from the due date": (5, 30),
        "Settled after 45 days from the due date": (0, 20),
    },

    "MLLimitTurn-over": [
        (0, 0, 0),
        (0.5, 4, 20),
        (1.25, 6, 30),
        (2, 7, 20),
        (3, 8, 10),
    ],

          ########################## Pre-shipment Export Credit(Loan stmnt) #########################

    "Average settlement from the date of advance in FCY": [
        
        (90, 7, 30),
        (120, 3, 20),
        (150, 3, 10),
    ],

    "PreETurn-over": [
        (0, 0, 0),
        (1, 2, 30),
        (1.25, 5, 30),
        (1.5, 7, 30),
        (2, 12, 20),
        (2.5, 13, 10),
    ],

          ########################## Post Export Credit Facility(Loan Stmnt) #########################

    "Average settlement": {
        "Settled within two weeks": (13, 10),
        "Settled within three weeks": (10, 40),
        "Settled within a month": (5, 30),
        "Settled after a month": (0, 20),
    },

    "PETurn-over": [
        (0, 0, 0),
        (1, 5, 30),
        (1.5, 7, 10),
        (2, 8, 20),
    ],

          ########################## Guarantee Facility(DDR) #########################

    "Settlement of Guarantee": {
        "Settled without claim": (21, 10),
        "Claimed but settled by customer": (15, 40),
        "Claimed and paid by the bank": (0, 30),
    },

          ########################## Credit Exposure(CIC) #########################

    "Aggregate Credit Exposure": [
        (100000, 1, 0),
        (1000000, 1.5, 0),
        (2000000, 3, 10),
        (3000000, 3.5, 40),
        (5000000, 4, 30),
    ],

############################################## Industry attractiveness  #########################
    
    "Credit Exposure to any one customer": {
        "Favorable": (4, 10),
        "Stable": (2, 40),
        "Unstable": (0, 30),
    },

    "Market competition/market": {
        "Dominant player": (3, 10),
        "Acceptable": (2, 40),
        "Weak Player": (0, 30),
    },

    "Form of Organization": {
        "Cooperatives": (3, 10),
        "Share Company": (2, 40),
        "Private limited company": (2, 30),
        "Sole proprietorship": (1, 20),
    },

############################################## Financial Position  #########################



    
    
       
}