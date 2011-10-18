# fork notes

1. Added preliminary threaded handler support, to process multiple requests per worker.

  Pass :threaded => true to the run() options to enable threaded dispatch.

  Pass :pool_size => int to run() options to control the thread pool size(default 10).

  Once pool is full of active handler threads, handler will block & not accept further zmq messages.

# khi-rack-mongrel2

The rack-mongrel2 gem was supposed to be "the only Mongrel2 Rack handler you'll ever need." Unfortunately, its original author hasn't done a lot of work with Mongrel2 or ØMQ, so parts of it made assumptions which just didn't work in actual production environments. He also seems to be pretty lazy about fixing obvious bugs which make the gem flat out unusable, and I've got work to do, so here's a fork.

# What's so Great About Yours?

1. Its reads are non-blocking.

2. It plays nice with ØMQ 2.1.x.

3. It handles SIGTERM gracefully.

4. I don't care about any of that Ruby hippie FFI shit. I use the standard-issue hand-compiled zmq gem and I like it. If you can't build it on your OS, you probably shouldn't be serving web pages from it.

5. I don't ever supply any default ØMQ connection specs since there is virtually NO chance that you will ever setup your Mongrel2 instance the same way I setup mine. Especially not if you're running more than one of them and can't keep recycling the same ports for every app you intend to boot. This does make the setup code look more like actual code than magic unicorn farts, so seeing it will probably drive some angry Rails teen to sperg out in a Peet's bathroom for a week while chanting "D.R.Y." and jerking off to the Kid Robot catalog he carries around in the $300 hardshell backpack his mom bought him to protect his Hario ceramic burr grinder from the mean streets of SoMa. I consider this my fork's greatest feature. 

6. I fixed a really stupid longstanding crasher that's been unaddressed for months which was hindering my team's progress.

7. It can actually be found by `gem which`.

# Sounds Cool. Gimme.

Add `http://gem.khiltd.com` to your gem sources--either via `gem source --add` or by throwing it in your Gemfile. Then just `gem install khi-rack-mongrel2` or `bundle install` and get rid of any potentially conflicting versions you might have been using previously. No, I won't be setting up a RubyForge account anytime soon.

# You Sound Like a Real Jerk

So don't give me one of your Zazzle cards.

## Copyright

Original project Copyright © 2010 Daniel Huckstep. See LICENSE for details.

This divergent, derivative work Copyright © 2011 KHI Ltd. Co., LLC. 
