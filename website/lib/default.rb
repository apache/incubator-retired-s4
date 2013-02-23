# All files in the 'lib' directory will be loaded
# before nanoc starts compiling.
include Nanoc3::Helpers::Rendering

def email(id, domain, label)
  "<script language='JavaScript'> 
		  <!-- // go away spammer!
				var name = '#{id}';
				var domain = '#{domain}';
				var label = '#{label}';
		        document.write('<a class=\"email\" href=\"mailto:' + name + '@' + domain + '\">');
		        document.write(label + '</a>');
		  // -->
	</script>"
end

def google_analytics(account_id, domain)
  "<script type='text/javascript'>
    var _gaq = _gaq || [];
    _gaq.push(['_setAccount', '#{account_id}']);
    _gaq.push(['_setDomainName', '#{domain}']);
    _gaq.push(['_trackPageview']);
    (function() {
      var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;
      ga.src = ('https:' == document.location.protocol ? 'https://ssl' : 'http://www') + '.google-analytics.com/ga.js';
      var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(ga, s);
    })();
  </script>
  "
end

def google_analytics_event(category, action, label)
  "_gaq.push(['_trackEvent', '#{category}', '#{action}', '#{label}']);"
end

def twitter_widget()
  "<script src='http://widgets.twimg.com/j/2/widget.js'></script>
  <script>
  new TWTR.Widget({
    version: 2,
    type: 'profile',
    rpp: 4,
    interval: 6000,
    width: 190,
    height: 300,
    theme: {
      shell: {
        background: '#e6e6e6',
        color: '#707070'
      },
      tweets: {
        background: '#ffffff',
        color: '#333333',
        links: '#7E2217'
      }
    },
    features: {
      scrollbar: false,
      loop: false,
      live: false,
      hashtags: true,
      timestamp: true,
      avatars: false,
      behavior: 'all'
    }
  }).render().setUser('s4project').start();
  </script>"
end