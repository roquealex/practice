from html.parser import HTMLParser

class TestParser(HTMLParser):
    def handle_starttag(self,tag,attrs):
        print("Start tag",tag)
        for attr in attrs :
            print(attr)

    def handle_endtag(self, tag):
        print("End tag",tag)

    def handle_data(self, data):
        print("Data:", data)

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
    </p>
</li>

</body>
</html>"""
parser.feed(html);

