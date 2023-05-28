$(document).ready(function() {
    // get the offset position of the section with the background image
    var sectionOffset = $('#home').offset().top;
  
    // add a scroll event listener to the window
    $(window).scroll(function() {
      // get the current scroll position
      var scrollPosition = $(this).scrollTop();
  
      // check if the scroll position is past the section with the background image
      if (scrollPosition > sectionOffset) {
        // add a class to the navbar to change its background color
        $('#navbar').addClass('bg-dark');
      } else {
        // remove the class from the navbar to restore its original background color
        $('#navbar').removeClass('bg-dark');
      }
    });
});

