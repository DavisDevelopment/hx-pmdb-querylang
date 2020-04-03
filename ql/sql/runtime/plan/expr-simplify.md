<div class="post-text" itemprop="text">
   <p>
   You probably want to implement a <a href="http://en.wikipedia.org/wiki/Rewriting" rel="noreferrer">term rewriting system</a>. Regarding the underlying math, have a look at <a href="http://en.wikipedia.org/wiki/Rewriting" rel="noreferrer">WikiPedia</a>.
   </p>

<p><strong>Structure of a term rewrite module</strong></p>

<p>Since I implemented a solution recently...</p>

<ul>
   <li><p>First, prepare a class CExpression, which models the structure of your expression.</p></li>
   <li><p>Implement <code>CRule</code>, which contains a pattern and a replacement. Use special symbols as pattern variables, which need to get bound during pattern matching and replaced in the replacement expression.</p></li>
   <li><p>Then, implement a class <code>CRule</code>. It's main method <code>applyRule(CExpression, CRule)</code> tries to match the rule against any applicable subexpression of expression. In case it matches, return the result.</p></li>
   <li><p>Finaly, implement a class <code>CRuleSet</code>, which is simply a set of CRule objects. The main method <code>reduce(CExpression)</code> applies the set of rules as long as no more rules can be applied and then returns the reduced expression.</p></li>
   <li><p>Additionally, you need a class <code>CBindingEnvironment</code>, which maps already matched symbols to the matched values. </p></li>
</ul>

<p><strong>Try to rewrite expression to a normal form</strong></p>

<p>Don't forget, that this approach works to a certain point, but is likely to be non complete. This is due to the fact, that all of the following rules perform local term rewrites. </p>

<p>To make this local rewrite logic stronger, one should try to transform expressions into something I'd call a normal form. This is my approach:</p>

<ul>
<li><p>If a term contains literal values, try to move the term as far to the right as possible. </p></li>
<li><p>Eventually, this literal value may appear rightmost and can be evaluated as part of a fully literal expression.</p></li>
</ul>

<p><strong>When to evaluate fully literal expression</strong></p>

<p>An interesting question is when to evaluate fully literal expression. Suppose you have an expression</p>

<pre><code>   x * ( 1 / 3 )
</code></pre>

<p>which would reduce to </p>

<pre><code>   x * 0.333333333333333333
</code></pre>

<p>Now suppose x gets replaced by 3. This would yield something like</p>

<pre><code>   0.999999999999999999999999
</code></pre>

<p>Thus eager evaluation returns a slightly incorrect value.</p>

<p>At the other side, if you keep ( 1 / 3 ) and first replace x by 3</p>

<pre><code>   3 * ( 1 / 3 )
</code></pre>

<p>a rewrite rule would give </p>

<pre><code>   1
</code></pre>

<p>Thus, it might be useful to evaluate fully literal expression late.</p>

<p><strong>Examples of rewrite rules</strong></p>

<p>Here is how my rules appear inside the application: The _1, _2, ... symbols match any subexpression:</p>

<pre><code>addRule( new TARuleFromString( '0+_1',   // left hand side  :: pattern
                               '_1'      // right hand side :: replacement
                             ) 
       );
</code></pre>

<p>or a bit more complicated</p>

<pre><code>addRule( new TARuleFromString( '_1+_2*_1', 
                               '(1+_2)*_1' 
                             ) 
       );
</code></pre>

<p>Certain special symbols only match special subexpressions. E.g. _Literal1, _Literal2, ... match only literal values:</p>

<pre><code>addRule( new TARuleFromString( 'exp(_Literal1) * exp(_Literal2 )', 
                               'exp( _Literal1 + _Literal2 )' 
                             ) 
       );
</code></pre>

<p>This rule moves non-literal expression to the left:</p>

<pre><code>addRule( new TARuleFromString( '_Literal*_NonLiteral', 
                               '_NonLiteral*_Literal' 
                             ) 
       );
</code></pre>

<p>Any name, that begins with a '_', is a pattern variable. While the system matches a rule, it keeps a stack of assignments of already matched symbols.</p>

<p>Finally, don't forget that rules may yield non terminating replacement sequences. 
Thus while reducing expression, make the process remember, which intermediate expressions have already been reached before. </p>

<p>In my implementation, I don't save intermediate expressions directly. I keep an array of MD5() hashes of intermediate expression.</p>

<p><strong>A set of rules as a starting point</strong></p>

<p>Here's a set of rules to get started:</p>

<pre><code>            addRule( new TARuleFromString( '0+_1', '_1' ) );
            addRule( new TARuleFromString( '_Literal2=0-_1', '_1=0-_Literal2' ) );
            addRule( new TARuleFromString( '_1+0', '_1' ) );

            addRule( new TARuleFromString( '1*_1', '_1' ) );
            addRule( new TARuleFromString( '_1*1', '_1' ) );

            addRule( new TARuleFromString( '_1+_1', '2*_1' ) );

            addRule( new TARuleFromString( '_1-_1', '0' ) );
            addRule( new TARuleFromString( '_1/_1', '1' ) );

            // Rate = (pow((EndValue / BeginValue), (1 / (EndYear - BeginYear)))-1) * 100 

            addRule( new TARuleFromString( 'exp(_Literal1) * exp(_Literal2 )', 'exp( _Literal1 + _Literal2 )' ) );
            addRule( new TARuleFromString( 'exp( 0 )', '1' ) );

            addRule( new TARuleFromString( 'pow(_Literal1,_1) * pow(_Literal2,_1)', 'pow(_Literal1 * _Literal2,_1)' ) );
            addRule( new TARuleFromString( 'pow( _1, 0 )', '1' ) );
            addRule( new TARuleFromString( 'pow( _1, 1 )', '_1' ) );
            addRule( new TARuleFromString( 'pow( _1, -1 )', '1/_1' ) );
            addRule( new TARuleFromString( 'pow( pow( _1, _Literal1 ), _Literal2 )', 'pow( _1, _Literal1 * _Literal2 )' ) );

//          addRule( new TARuleFromString( 'pow( _Literal1, _1 )', 'ln(_1) / ln(_Literal1)' ) );
            addRule( new TARuleFromString( '_literal1 = pow( _Literal2, _1 )', '_1 = ln(_literal1) / ln(_Literal2)' ) );
            addRule( new TARuleFromString( 'pow( _Literal2, _1 ) = _literal1 ', '_1 = ln(_literal1) / ln(_Literal2)' ) );

            addRule( new TARuleFromString( 'pow( _1, _Literal2 ) = _literal1 ', 'pow( _literal1, 1 / _Literal2 ) = _1' ) );

            addRule( new TARuleFromString( 'pow( 1, _1 )', '1' ) );

            addRule( new TARuleFromString( '_1 * _1 = _literal', '_1 = sqrt( _literal )' ) );

            addRule( new TARuleFromString( 'sqrt( _literal * _1 )', 'sqrt( _literal ) * sqrt( _1 )' ) );

            addRule( new TARuleFromString( 'ln( _Literal1 * _2 )', 'ln( _Literal1 ) + ln( _2 )' ) );
            addRule( new TARuleFromString( 'ln( _1 * _Literal2 )', 'ln( _Literal2 ) + ln( _1 )' ) );
            addRule( new TARuleFromString( 'log2( _Literal1 * _2 )', 'log2( _Literal1 ) + log2( _2 )' ) );
            addRule( new TARuleFromString( 'log2( _1 * _Literal2 )', 'log2( _Literal2 ) + log2( _1 )' ) );
            addRule( new TARuleFromString( 'log10( _Literal1 * _2 )', 'log10( _Literal1 ) + log10( _2 )' ) );
            addRule( new TARuleFromString( 'log10( _1 * _Literal2 )', 'log10( _Literal2 ) + log10( _1 )' ) );

            addRule( new TARuleFromString( 'ln( _Literal1 / _2 )', 'ln( _Literal1 ) - ln( _2 )' ) );
            addRule( new TARuleFromString( 'ln( _1 / _Literal2 )', 'ln( _Literal2 ) - ln( _1 )' ) );
            addRule( new TARuleFromString( 'log2( _Literal1 / _2 )', 'log2( _Literal1 ) - log2( _2 )' ) );
            addRule( new TARuleFromString( 'log2( _1 / _Literal2 )', 'log2( _Literal2 ) - log2( _1 )' ) );
            addRule( new TARuleFromString( 'log10( _Literal1 / _2 )', 'log10( _Literal1 ) - log10( _2 )' ) );
            addRule( new TARuleFromString( 'log10( _1 / _Literal2 )', 'log10( _Literal2 ) - log10( _1 )' ) );


            addRule( new TARuleFromString( '_Literal1 = _NonLiteral + _Literal2', '_Literal1 - _Literal2 = _NonLiteral' ) );
            addRule( new TARuleFromString( '_Literal1 = _NonLiteral * _Literal2', '_Literal1 / _Literal2 = _NonLiteral' ) );
            addRule( new TARuleFromString( '_Literal1 = _NonLiteral / _Literal2', '_Literal1 * _Literal2 = _NonLiteral' ) );
            addRule( new TARuleFromString( '_Literal1 =_NonLiteral - _Literal2',  '_Literal1 + _Literal2 = _NonLiteral' ) );

            addRule( new TARuleFromString( '_NonLiteral + _Literal2 = _Literal1 ', '_Literal1 - _Literal2 = _NonLiteral' ) );
            addRule( new TARuleFromString( '_NonLiteral * _Literal2 = _Literal1 ', '_Literal1 / _Literal2 = _NonLiteral' ) );
            addRule( new TARuleFromString( '_NonLiteral / _Literal2 = _Literal1 ', '_Literal1 * _Literal2 = _NonLiteral' ) );
            addRule( new TARuleFromString( '_NonLiteral - _Literal2 = _Literal1',  '_Literal1 + _Literal2 = _NonLiteral' ) );

            addRule( new TARuleFromString( '_NonLiteral - _Literal2 = _Literal1 ', '_Literal1 + _Literal2 = _NonLiteral' ) );
            addRule( new TARuleFromString( '_Literal2 - _NonLiteral = _Literal1 ', '_Literal2 - _Literal1 = _NonLiteral' ) );

            addRule( new TARuleFromString( '_Literal1 = sin( _NonLiteral )', 'asin( _Literal1 ) = _NonLiteral' ) );
            addRule( new TARuleFromString( '_Literal1 = cos( _NonLiteral )', 'acos( _Literal1 ) = _NonLiteral' ) );
            addRule( new TARuleFromString( '_Literal1 = tan( _NonLiteral )', 'atan( _Literal1 ) = _NonLiteral' ) );

            addRule( new TARuleFromString( '_Literal1 = ln( _1 )', 'exp( _Literal1 ) = _1' ) );
            addRule( new TARuleFromString( 'ln( _1 ) = _Literal1', 'exp( _Literal1 ) = _1' ) );

            addRule( new TARuleFromString( '_Literal1 = _NonLiteral', '_NonLiteral = _Literal1' ) );

            addRule( new TARuleFromString( '( _Literal1 / _2 ) = _Literal2', '_Literal1 / _Literal2 = _2 ' ) );

            addRule( new TARuleFromString( '_Literal*_NonLiteral', '_NonLiteral*_Literal' ) );
            addRule( new TARuleFromString( '_Literal+_NonLiteral', '_NonLiteral+_Literal' ) );

            addRule( new TARuleFromString( '_Literal1+(_Literal2+_NonLiteral)', '_NonLiteral+(_Literal1+_Literal2)' ) );
            addRule( new TARuleFromString( '_Literal1+(_Literal2+_1)', '_1+(_Literal1+_Literal2)' ) );

            addRule( new TARuleFromString( '(_1*_2)+(_3*_2)', '(_1+_3)*_2' ) );
            addRule( new TARuleFromString( '(_2*_1)+(_2*_3)', '(_1+_3)*_2' ) );

            addRule( new TARuleFromString( '(_2*_1)+(_3*_2)', '(_1+_3)*_2' ) );
            addRule( new TARuleFromString( '(_1*_2)+(_2*_3)', '(_1+_3)*_2' ) );

            addRule( new TARuleFromString( '(_Literal * _1 ) / _Literal', '_1' ) );
            addRule( new TARuleFromString( '(_Literal1 * _1 ) / _Literal2', '(_Literal1 * _Literal2 ) / _1' ) );

            addRule( new TARuleFromString( '(_1+_2)+_3', '_1+(_2+_3)' ) );
            addRule( new TARuleFromString( '(_1*_2)*_3', '_1*(_2*_3)' ) );

            addRule( new TARuleFromString( '_1+(_1+_2)', '(2*_1)+_2' ) );

            addRule( new TARuleFromString( '_1+_2*_1', '(1+_2)*_1' ) );

            addRule( new TARuleFromString( '_literal1 * _NonLiteral = _literal2', '_literal2 / _literal1 = _NonLiteral' ) );
            addRule( new TARuleFromString( '_literal1 + _NonLiteral = _literal2', '_literal2 - _literal1 = _NonLiteral' ) );
            addRule( new TARuleFromString( '_literal1 - _NonLiteral = _literal2', '_literal1 - _literal2 = _NonLiteral' ) );
            addRule( new TARuleFromString( '_literal1 / _NonLiteral = _literal2', '_literal1 * _literal2 = _NonLiteral' ) );
</code></pre>

<p><strong>Make rules first-class expressions</strong></p>

<p>An interesting point: Since the above rules are special expression, which get correctly evaluate by the expression parser, users can even add new rules and thus enhance the application's rewrite capabilities.</p>

<p><strong>Parsing expressions (or more general: languages)</strong></p>

<p>For <strong>Cocoa/OBjC applications</strong>, <a href="https://github.com/davedelong/DDMathParser" rel="noreferrer">Dave DeLong's DDMathParser</a> is a perfect candidate to syntactically analyse mathematical expressions. </p>

<p>For other languages, our old friends <a href="http://dinosaur.compilertools.net/" rel="noreferrer">Lex &amp; Yacc</a> or the newer <a href="http://www.gnu.org/software/bison/" rel="noreferrer">GNU Bison</a> might be of help.</p>

<p>Far younger and with an <a href="http://www.antlr.org/grammar/list" rel="noreferrer">enourmous set of ready to use syntax-files</a>, <a href="http://www.antlr.org/" rel="noreferrer">ANTLR</a> is a modern parser generator based on Java. Besides purely command-line use, <a href="http://www.antlr.org/works/index.html" rel="noreferrer">ANTLRWorks</a> provides a <strong>GUI frontend</strong>  to construct and debug ANTLR based parsers. ANTLR generates grammars for <a href="http://www.antlr.org/wiki/display/ANTLR3/Code+Generation+Targets" rel="noreferrer">various host language</a>, like <a href="http://www.antlr.org/wiki/display/ANTLR3/Code+Generation+Targets" rel="noreferrer">JAVA, C, Python, PHP or C#</a>. The ActionScript runtime is currently <a href="https://stackoverflow.com/questions/6480598/problems-building-antlr-v3-3-from-source-antlr3-maven-archetype-missing/6481934#comment-7627243">broken</a>.</p>

<p>In case you'd like to <strong>learn how to parse expressions</strong> (or languages in general) from the bottom-up, I'd propose this <a href="http://www.ethoberon.ethz.ch/WirthPubl/CBEAll.pdf" rel="noreferrer">free book's text from Niklaus Wirth</a> (or the <a href="http://amzn.to/q8rJTF" rel="noreferrer">german book edition</a>), the famous inventor of Pascal and Modula-2.</p>
    </div>