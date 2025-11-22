<center><strong>PROJECT: GRAHAM CRACKER</strong></center> <br>
<center><i>Breaking Data into Digestible Pieces</i></center> <br>

By: Pavi Gupta, Liana Kim, Sae-Jin Moon, Anh Phan, Daniel Son, Esther Yun 

**Executive Summary** 
A CSV (comma-separated values) file is a plain text file that stores tables of data and organizes with rows and columns. These files are important in the world of data because they are simple, universally compatible, and easy to store, transfer, and share. However, these files often contain a jumble of unorganized data that makes it hard to analyze and understand.  

Through Project GRAHAM Cracker, we hope to help data scientists clean up their files. Users will have the ability to upload their file and pick parameters that they want their data to be organized by. For example, they can select an option for the data to be organized to remove any duplicate cells. They will then be able to download the organized file in a variety of formats to use and analyze in any way they see fit. The frontend is very accessible and easy to understand, allowing anyone from a beginner coder to a seasoned data scientist to customize and organize their data. 

**System Overview**
Some concepts and tools we used includes: 
- GitHub, CSVkit, importing CSV files (from Case 3) 
- Creating an app.py, flask, and API (from Case 4) 
- Building a front end and back end (from Case 7) 

<ins>Architecture</ins>

<img width="992" height="458" alt="Image" src="https://github.com/user-attachments/assets/cbfde054-f85f-449e-b642-c9ce5467cb48" />

**How to Run**

**Design Decisions**

**Results & Evaluation**
<ins>What the front end looks like from the user's perspective</ins>

<ins>Feature: Selecting columns in the CSV to ignore</ins>

<ins>Feature: Filter expression</ins>

<ins>Feature: Removing duplicate rows</ins>

<ins>Feature: Allowing user to download a compressed version of the CSV file</ins>

<ins>Feature: View results of the changes and download the file for personal use</ins>

**What's next?**
In the future, we can improve this by adding more features for the user to access. Some of these features include: 
- Changing language and grammar in the data to be more legible without changing the meaning of it. 
- Standardizing capitalization across the cells (ex. Headers and titles become uppercase and cells are lowercase) 
- Profiling the columns: Printing out the different types of data, how many cells are empty, etc. 
- Stretch features: Allowing the user to upload more types of files than just CSV files, allow the user to create an account that stores all the files that they have uploaded for their storage.  

Code refactors: 
- Caching some values input by the user so they do not need to select their preferences repeatedly every time they refresh their page. This will lead to a more seamless, improved user experience.  
- Making major aesthetic changes to make it more uniform, improve light/dark mode settings, and increase accessibility for color blind users.  

**Links**
GitHub: https://github.com/saejin-moon/ds2022-final 
