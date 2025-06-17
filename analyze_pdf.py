import pdfplumber
import re 

# Only show errors, not warnings 
import logging
logging.getLogger("pdfminer").setLevel(logging.ERROR) 

# Lists of tests given in English or Spanish. These should be exhaustive, 
# (will not cause issues if a certain test not given for a particular child)
EN_TESTS = [
    "VISUAL PROCESSING (Gv)",
    "Story Recall",
    "Visualization",
    "Numbers Reversed",
    "Picture Recognition",
    "Pair Cancellation",
    "Applied Problems"
]

SP_TESTS = [
    "Rememoración de cuentos",
    "Visualización",
    "Inversión de números",
    "Cancelación de pares",
    "Problemas aplicados"
]

#  Lists of metrics to check for, these should match exactly
EN_METRICS = [
    "W", "AE", "RPI", "Proficiency", "SS 95% Band", "PR", "T"  
]

SP_METRICS = [
    "W", "AE", "RPI", "SS 95% Band" 
]

# Lists of proficiency levels that have whitespace. We will remove the whitespace
# in order to split the text on whitespace. 
# (will not cause issues if a profiency level not given to a particular child)
EN_PROFICIENCY_LEVELS_WITH_WHITESPACE = [
    "Very Advanced",
    "Average to Advanced",
    "Limited to Average",
    "Very Limited",
    "Extremely Limited"
]

# Currently not being applied
SP_PROFICIENCY_LEVELS_WITH_WHITESPACE = []

# Names of observation sections (used in dictionary keys, titles of csv output)
# same for both english and spanish
OBS_SECTIONS = (
    "cognitive_obs", 
    "achievement_obs",
    "qualitative_obs"
)

# Exact text to match on observation section headers 
EN_OBS_SECTION_HEADERS = (
    "Woodcock-Johnson IV Tests of Cognitive Abilities Test Session Observations",
    "Woodcock-Johnson IV Tests of Achievement Form A and Extended Test Session Observations",
    "Woodcock-Johnson IV Tests of Achievement Form A and Extended Qualitative Observations"
)

SP_OBS_SECTION_HEADERS = (
   "Batería IV Woodcock-Muñoz Pruebas de habilidades cognitivas Test Session Observations",
   "Batería IV Woodcock-Muñoz Pruebas de aprovechamiento Test Session Observations",
   "Batería IV Woodcock-Muñoz Pruebas de aprovechamiento Qualitative Observations" 
)

# Part of the section header string,
# Used to decide when to move onto a new section, when this text is found  
EN_OBS_SECTION_HEADER_GENERIC = "Woodcock-Johnson IV Tests"
SP_OBS_SECTION_HEADER_GENERIC = "Batería IV Woodcock-Muñoz Pruebas"

class ReportScraper():
    
    def __init__(self, path: str):
        
        self.data = {}
            
        # Open file 
        with pdfplumber.open(path) as pdf:
            self.text = []
            for page in pdf.pages:
                self.text.extend(page.extract_text().split("\n"))
            
        # Find language of report 
        self.language = "English"
        for line in self.text:
            if "Batería" in line:
                self.language = "Spanish"
                break
        
        # Find last line of report 
        self.last_line = len(self.text) - 1
        
        # Plumb PDF in English or Spanish
        if self.language == "English":
            self.tests = EN_TESTS
            self.metrics = EN_METRICS
            self.proficiency_levels = EN_PROFICIENCY_LEVELS_WITH_WHITESPACE
            self.obs_section_header_generic = EN_OBS_SECTION_HEADER_GENERIC
            
            (self.cognitive_obs_header,  
            self.achievement_obs_header,  
            self.qualitative_obs_header) = EN_OBS_SECTION_HEADERS
            
            
        else:
            self.tests = SP_TESTS  
            self.metrics = SP_METRICS
            self.proficiency_levels = SP_PROFICIENCY_LEVELS_WITH_WHITESPACE
            self.obs_section_header_generic = SP_OBS_SECTION_HEADER_GENERIC
            
            (self.cognitive_obs_header,  
            self.achievement_obs_header,  
            self.qualitative_obs_header) = SP_OBS_SECTION_HEADERS  
        
        # Line #s for scores, three sets of observations
        for i, line in enumerate(self.text):
            if line == "TABLE OF SCORES":
                self.scores_line = i + 1
            elif line == self.cognitive_obs_header:
                self.cognitive_obs_first_line = i + 1
            elif line == self.achievement_obs_header:
                self.achievement_obs_first_line = i + 1
            elif line == self.qualitative_obs_header:
                self.qualitative_obs_first_line = i + 1
        
        # Find the first line with observations
        self.first_obs_line = min(getattr(self, "cognitive_obs_first_line", self.last_line),
                                  getattr(self, "achievement_obs_first_line", self.last_line),
                                  getattr(self, "qualitative_obs_first_line", self.last_line),
        )
        
        # For tests in english, scores are finished at start of observations,
        # For tests in spanish, scores are at end of the report 
        if self.language == "English":
            self.end_of_scores_line = self.first_obs_line
        else:
            self.end_of_scores_line = self.last_line 

                 
    def print_file(self):
        for i, line in enumerate(self.text):
            print(i, line)
            
    def __str__(self):
        return "\n".join(f"{k}: {v}" for k, v in self.data.items())
    
    def get_headers(self):
        # This section of the PDF is split into lines consisting of two columns
        header_lines = ([
            (1, "Name", "School"),
            (2, "Date of Birth", "Teacher"),
            (3, "Age", "Grade"),
            (4, "Sex", "ID"),
            (5, "Date of Testing", "Examiners"),
        ])

        # Loop through each row of header cols
        for line in header_lines:
            i, var1, var2 = line
            
            # First column in row 
            match1 = re.search(fr"{var1}:(.*?){var2}:", self.text[i])
            value1 = match1.group(1).strip() if match1 else None

            # If age, clean by converting to months (12 * years + months)
            if var1 == "Age":
                age_vals = value1.split(", ") # Split into years, months
                value1 = str(int(age_vals[0][0]) * 12 + 
                             int(age_vals[1][0])
                ) + " Months"
              
            # If date of testing, remove anything after MM/DD/YYYY  
            elif var1 == "Date of Testing":
                value1 = value1.split()[0]
            
            # Second column in row 
            match2 = re.search(fr"{var2}:(.*)", self.text[i])
            value2 = match2.group(1).strip() if match2 else None
            
            # Save in dictionary 
            self.data[var1] = value1
            self.data[var2] = value2
            
        # Also include language to be helpful 
        self.data["Language"] = self.language
        
            
    def set_id(self, id_key: str = "Name"):
        # Set ID equal to other variable (that is, a key on the data dict)
        self.data["ID"] = re.split(r'[^\w]+', self.data[id_key])[0]
        
        
    def get_test_scores(self):
        for test in self.tests:   
            for line in self.text[self.scores_line: self.end_of_scores_line]:
                if line.startswith(test):
                    ## Pre-process text  
                    # remove white space before parenthesis
                    scores = line.replace(" (", "(")
                    
                    # Also replace spaces with underscores for proficiency lvls
                    pattern = re.compile(r'\b(' + 
                                         '|'.join(map(re.escape, self.proficiency_levels)) +
                                         r')\b')
                    scores = pattern.sub(lambda m: m.group(1).replace(" ", "_"), 
                                         scores)
                    
                    # Remove name of test from scores 
                    scores = scores.replace(test,"").split() 
                    
                    # Save metric/scores
                    for metric, score in zip(self.metrics, scores):
                        
                        ## Post-process text 
                        # convert underlines back to spaces
                        score = score.replace("_", " ") 
                        
                        # add space back in before parentheses 
                        score = score.replace("(", " (") 
                
                        # Save language, test, metric, and score
                        self.data[f"{self.language} - {test.title()} - {metric.strip()}"] = score.strip()
                    
                    break # since we found the test in sheet, don't keep looking
                
    def get_observations(self):
        for obs_section in OBS_SECTIONS:
            
            # Check if observation section exists in report
            first_line_key = f"{obs_section}_first_line"
            obs_first_line = getattr(self, first_line_key, None)
            
            if obs_first_line:
                skip_counter = 0
                # Loop through lines starting at that section of report 
                lines_to_check = self.text[obs_first_line:]
                for i, line in enumerate(lines_to_check):
                    
                    # If needed, skip to next line  
                    if skip_counter:
                        skip_counter -= 1 
                        continue
                    
                    # Check for "reason for poor sample" lines, which are long
                    # and trip up parsing below
                    if (line.startswith("The results of the WJ IV Tests of") or
                        line.startswith("The results of the Batería IV Woodcock-Muñoz Pruebas")
                        ):
                            # First, combine line with next line
                            lines_combined = line + " " + lines_to_check[i+1]
                            
                            # Get response value, the info after :
                            resp = ":".join(lines_combined.split(":")[1:])
                            
                            # Reconstruct line with this response 
                            line = "Poor Sample: " + resp
                            skip_counter = 1
                    
                    # Check if reached next observation section, 
                    # if so, move to it
                    if line.startswith(self.obs_section_header_generic):
                        break 
                    
                    # If reached end of page, skip 5 lines (footer/header info)
                    elif line.startswith(("1 of", "2 of", "3 of")):
                        skip_counter = 4 # Skip this line + 4 more 
                        continue # if end of report, this will end the loop
                    
                    # If not reached next section nor end of page, check that 
                    # this line starts an observation, identified via a colon (:)
                    # If no colon, that means continuation of last line, can skip
                    elif not ":" in line:
                        continue
        
                    else:
                        # Otherwise, proceed as normal, getting observation 
                        line_split = line.split(":")
                        obs_type = line_split[0]
                        obs_val = ":".join(line_split[1:])
                        
                        # Grab next line (won't cause error, as we know we arent
                        # at the end of the page)
                        next_line = lines_to_check[i+1]
                                
                        # If the next line is the end of the page 
                        if ("1 of " in next_line or 
                            "2 of " in next_line or 
                            "3 of " in next_line):
                            # Then check that next line isn't end of report
                            # Do this by checking for "Page 2 of 2" for example
                            if next_line != f"{next_line[0]} of {next_line[0]}":
                                # If not end of report, check 6 lines ahead 
                                next_line = lines_to_check[i+6]
                                if not ":" in next_line:
                                    if not self.obs_section_header_generic in next_line:
                                    # If next line isn't a new observation, and 
                                    # is not next section, then add spillover 
                                    # text to observation value 
                                        obs_val += " " + next_line
                                
                        elif not ":" in next_line:
                            if not self.obs_section_header_generic in next_line:
                            # (Same as above), add any spillover text
                                obs_val += " " + next_line
                        
                        # Set observation type equal to observation value 
                        self.data[f"{self.language} - {obs_section.replace("_", " ").title()}: {obs_type.strip()}"] = obs_val.strip()                          

if __name__ == "__main__":
    pass

