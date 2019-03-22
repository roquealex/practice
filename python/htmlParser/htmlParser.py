from html.parser import HTMLParser
from enum import Enum

class States(Enum):
    IDLE = 0
    PROCESS_ROW = 1
    EXPECT_PRICE = 2
    EXPECT_HOOD = 3
    EXPECT_TITLE = 4

class TestParser(HTMLParser):

    def __TestParser__tag_IDLE(self,tag,attrDict):
        print("IDLE method")
        if tag == "li" :
            #localId = 0;
            
            #self.id = attrDict.get("data-pid")
            if (attrDict.get("class")== "result-row"):
                # must be here or error
                self.id = attrDict["data-pid"]
                self.processLevel = self.liLevel
                self.state = States.PROCESS_ROW

    def __TestParser__tag_PROCESS_ROW(self,tag,attrDict):
        print("PROCESS_ROW");
        if tag == "span" :
            if (attrDict.get("class")== "result-price"):
                self.state = States.EXPECT_PRICE
            if (attrDict.get("class")== "result-hood"):
                self.state = States.EXPECT_HOOD
        if tag == "a" :
            #if (attrDict.get("class")== "result-title" in attrDict.get(class)):
            #if (attrDict.get("class")== "result-title hdrlnk"):
            if ("result-title" in attrDict.get("class")):
                self.state = States.EXPECT_TITLE
        #<a href="https://sfbay.craigslist.org/eby/msg/d/oakley-great-sounding-guitar/6844377829.html" data-id="6844377829" class="result-title hdrlnk">GREAT SOUNDING GUITAR</a>
        #if tag == "a" :


    def __TestParser__tag_UNDEF(self,tag,attrDict):
        print("UNDEF");
        raise Exception("Undefined tag process")

    def __init__(self):
        HTMLParser.__init__(self)
        self.state = States.IDLE

        self.id = None
        self.price = None
        self.hood = None
        self.title = None

        self.liLevel = 0;
        self.processLevel = -1;
        self.tag_state = {
            States.IDLE: self.__TestParser__tag_IDLE,
            States.PROCESS_ROW: self.__TestParser__tag_PROCESS_ROW,
            States.EXPECT_PRICE: self.__TestParser__tag_UNDEF,
            States.EXPECT_HOOD: self.__TestParser__tag_UNDEF,
            States.EXPECT_TITLE: self.__TestParser__tag_UNDEF
            }
        
    def handle_starttag(self,tag,attrs):
        if tag == "li" :
            self.liLevel = self.liLevel + 1;
        print("Start tag",tag)
        attrDict = dict(attrs)
        self.tag_state[self.state](tag,attrDict)
        for (key,value) in attrs :
            print(key,"->",value)
        print(dict(attrs))

    def handle_endtag(self, tag):
        print("End tag",tag)
        if tag == "li" :
            if self.processLevel == self.liLevel :
                # write the row and cleanup
                print("COLLECTED:",self.id,self.price,self.hood,self.title)
                self.state = States.IDLE
                self.id = None
                self.price = None
                self.hood = None
                self.title = None
            self.liLevel = self.liLevel - 1;
        if tag == "span" :
            if self.state == States.EXPECT_PRICE :
                assert self.price != None, "Price data is expected to be collected by now"
                self.state = States.PROCESS_ROW
            if self.state == States.EXPECT_HOOD :
                assert self.hood != None, "Hood data is expected to be collected by now"
                self.state = States.PROCESS_ROW
        if tag == "a" :
            if self.state == States.EXPECT_TITLE :
                assert self.title != None, "Title data is expected to be collected by now"
                self.state = States.PROCESS_ROW
                

    def handle_data(self, data):
        print("Data:", data)
        if self.state == States.EXPECT_PRICE :
            if self.price == None :
                self.price = data
            else :
                assert self.price == data, "There were 2 different prices found"
        if self.state == States.EXPECT_HOOD :
            if self.hood == None :
                self.hood = data
            else :
                assert self.hood == data, "There were 2 different hoods found"
        if self.state == States.EXPECT_TITLE :
            if self.title == None :
                self.title = data
            else :
                assert self.title == data, "There were 2 different titles found"
        

        

parser = TestParser()


html = """<html>
<head>
<title>Test</title>
</head>
<body>
<h1>Parse me!</h1>

         <li class="result-row" data-pid="6844377829">

        <a href="https://sfbay.craigslist.org/eby/msg/d/oakley-great-sounding-guitar/6844377829.html" class="result-image gallery" data-ids="1:00z0z_h26nUJz7zKf,1:00X0X_4IOMxJqepaC,1:00q0q_c6GaG5DhlIZ,1:00g0g_kUDXEPtIWK7,1:00h0h_iC2gW7pw0SO,1:01313_9KZHt4ZsbmY,1:00B0B_3iX6o5HhPuA,1:00H0H_3nNv9HcZYGW,1:00z0z_jGwnLcouplL,1:00z0z_h26nUJz7zKf">
                <span class="result-price">$200</span>
        </a>

    <p class="result-info">
        <span class="icon icon-star" role="button">
            <span class="screen-reader-text">favorite this post</span>
        </span>

            <time class="result-date" datetime="2019-03-20 16:39" title="Wed 20 Mar 04:39:43 PM">Mar 20</time>


        <a href="https://sfbay.craigslist.org/eby/msg/d/oakley-great-sounding-guitar/6844377829.html" data-id="6844377829" class="result-title hdrlnk">GREAT SOUNDING GUITAR</a>


        <span class="result-meta">
                <span class="result-price">$200</span>


                <span class="result-hood"> (brentwood / oakley)</span>

                <span class="result-tags">
                    pic
                    <span class="maptag" data-pid="6844377829">map</span>
                </span>

                <span class="banish icon icon-trash" role="button">
                    <span class="screen-reader-text">hide this posting</span>
                </span>

            <span class="unbanish icon icon-trash red" role="button" aria-hidden="true"></span>
            <a href="#" class="restore-link">
                <span class="restore-narrow-text">restore</span>
                <span class="restore-wide-text">restore this posting</span>
            </a>

        </span>
        more test <br>
        a little more <br>
        once more <br>
        end
    </p>
    <li></li>
</li>
    <li></li>
<p>another tag</p>

</body>
</html>"""
parser.feed(html);

